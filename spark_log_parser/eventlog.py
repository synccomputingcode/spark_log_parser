import gzip
import json
import pandas as pd
import shutil
import struct
import tarfile
import zipfile
import boto3 as boto
import requests
from pathlib import Path
from pydantic import BaseModel, root_validator, stricturl


AllowedURLs = stricturl(host_required=False, allowed_schemes={"https", "s3", "file"})


class EventLog(BaseModel):
    url: AllowedURLs
    work_dir: Path
    event_log: Path | None
    file_total = 0
    size_total = 0
    file_limit = 15
    size_limit = 5000000000
    compression_ratio_limit = 100

    @root_validator()
    def validate_event_log(cls, values):
        return vars(EventLogBuilder(values['url'], values['work_dir'], values['file_limit'], values['size_limit'], values['compression_ratio_limit']).build())


class EventLogBuilder:
    def __init__(self, url, work_dir, file_limit, size_limit, compression_ratio_limit):
        self.url = url
        self.work_dir = work_dir
        self.file_total = 0
        self.size_total = 0
        self.file_limit = file_limit
        self.size_limit = size_limit
        self.compression_ratio_limit = compression_ratio_limit


    def build(self) -> "EventLogBuilder":
        if self.url.scheme in {"https", "s3"}:
            local_path = self.work_dir.joinpath(self._basename())
            self._download(local_path)
        else:
            local_path = Path(self.url.path)

        event_logs = self._extract_archive(local_path, self.work_dir)

        # Remove the file we downloaded to free up space
        if self.url.scheme in {"https", "s3"}:
            local_path.unlink()

        self.event_log = self._concat(event_logs)

        return self


    def _extract_archive(self, event_log: Path, extract_dir: Path | None = None):
        extension = "".join(event_log.suffixes)
        if self._is_supported_archive_extension(extension):
            if not extract_dir:
                extract_dir = event_log.parent

            if extension.endswith(".tar.gz"):
                return self._extract_tgz(event_log, extract_dir.joinpath(event_log.name[:-len(".tar.gz")]))
            if extension.endswith(".tgz"):
                return self._extract_tgz(event_log, extract_dir.joinpath(event_log.name[:-len(".tgz")]))
            if extension.endswith(".zip"):
                return self._extract_zip(event_log, extract_dir.joinpath(event_log.name[:-len(".zip")]))
            if extension == ".gz":
                return self._extract_gz(event_log, extract_dir.joinpath(event_log.name[:-len(".gz")]))
            if extension in {".json", ".log", ""}:
                return [event_log]

            raise ValueError("Unsupported extension")

        else:
            return [event_log]


    def _concat(self, event_logs: list[Path]):
        if len(event_logs) == 1:
            return event_logs[0]

        dat = []
        for log in event_logs:
            with open(log) as log_file:
                line = json.loads(log_file.readline())
                if line['Event'] == "DBCEventLoggingListenerMetadata":
                    dat.append((line['Rollover Number'], line['SparkContext Id'], log))

        df = pd.DataFrame(dat, columns=["rollover_index", "context_id", "path"]).sort_values("rollover_index")
        
        if not all(df.rollover_index.diff()[1:] == 1):
            raise ValueError("Rollover file appears to be missing")

        if not len(df.context_id.unique() == 1):
            raise ValueError("Not all rollover files have the same Spark context ID")

        event_log = self.work_dir.joinpath(self._basename() + "-concatenated.json")
        with open(event_log, "w") as fobj:
            for path in df.path:
                with open(path) as part_fobj:
                    for line in part_fobj:
                        fobj.write(line)

        return event_log


    def _extract_tgz(self, archive_path: Path, extract_dir: Path):
        paths = []

        with tarfile.open(archive_path) as tar_file:
            for tf in tar_file:
                if self._should_skip_file(tf.name):
                    continue
                if tf.isdir():
                    continue

                self.file_total += 1
                self.size_total += tf.size
                self._safety_check(tf.size)

                extract_path = extract_dir.joinpath(tf.name)
                extract_path.parent.mkdir(parents=True, exist_ok=True)
                with tar_file.extract(tf.name) as source, open(extract_path, "wb") as target:
                    shutil.copyfileobj(source, target)

                # Remove the archive if it was extracted to free up space
                sub_paths = self._extract_archive(extract_path)
                if extract_path not in sub_paths:
                    extract_path.unlink()

                paths += sub_paths

        return paths


    def _extract_zip(self, archive_path: Path, extract_dir: Path):
        paths = []

        with zipfile.ZipFile(archive_path) as zfile:
            for zinfo in zfile.infolist():
                if self._should_skip_file(zinfo.filename):
                    continue
                if zinfo.is_dir():
                    continue

                self.file_total += 1
                self.size_total += zinfo.file_size
                self._safety_check(zinfo.file_size)

                extract_path = extract_dir.joinpath(zinfo.filename)
                extract_path.parent.mkdir(parents=True, exist_ok=True)
                with zfile.open(zinfo.filename) as source, open(extract_path, "wb") as target:
                    shutil.copyfileobj(source, target)

                # Remove the archive if it was extracted to free up space
                sub_paths = self._extract_archive(extract_path)
                if extract_path not in sub_paths:
                    extract_path.unlink()

                paths += sub_paths

        return paths


    def _extract_gz(self, archive_path: Path, extract_path: Path):
        with open(archive_path, "rb") as fobj:
            fobj.seek(-4, 2)
            size = struct.unpack("I", fobj.read(4))[0]

        self.size_total += size
        self._safety_check(size)

        with gzip.open(archive_path, "rb") as source, open(extract_path, "wb") as target:
            shutil.copyfileobj(source, target)

        return [extract_path]


    def _is_supported_archive_extension(self, extension: str):
        if extension in {".tar.gz", ".tgz", ".gz", ".zip"}:
            return True
        return False


    FILE_SKIP_PATTERNS = [
        ".DS_Store".lower(),
        "__MACOSX".lower(),
        "/."
    ]

    def _should_skip_file(self, filename: str):
        if filename.startswith("."):
            return True

        filename = filename.lower()
        for name in EventLogBuilder.FILE_SKIP_PATTERNS:
            if name in filename:
                return True

        return False


    def _safety_check(self, size):
        ratio = size / self.size_total
        if ratio > self.compression_ratio_limit:
            raise ValueError("Suspicious ratio size")

        if self.size_total > self.size_limit:
            raise ValueError("Arhive too big")

        if self.file_total > self.file_limit:
            raise ValueError("Too many filesin archive")


    def _download(self, path):
        with open(path, 'wb') as fobj:
            if self.url.scheme == "https":
                response = requests.get(self.url, stream=True)
                for chunk in response.iter_content():
                        fobj.write(chunk)
            elif self.url.scheme == "s3":
                s3 = boto.client("s3")
                s3.download_fileobj(self.url.host, self.url.path, fobj)

        return path


    def _basename(self):
        if self.url.scheme in {"https", "s3"}:
            return self.url.path.split("/")[-1]
        elif self.url.scheme == "file":
            return Path(self.url.path).name

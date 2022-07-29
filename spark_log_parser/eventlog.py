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


AllowedURL = stricturl(host_required=False, tld_required=False, allowed_schemes={"https", "s3", "file"})


class EventLog(BaseModel):
    source_url: AllowedURL
    work_dir: Path
    event_log: Path | None
    file_total = 0
    size_total = 0
    file_limit = 15
    size_limit = 5000000000
    compression_ratio_limit = 100

    @root_validator()
    def validate_event_log(cls, values):
        return vars(EventLogBuilder(values['source_url'], values['work_dir'], values['file_limit'], values['size_limit'], values['compression_ratio_limit']).build())


class EventLogBuilder:
    def __init__(self, source_url, work_dir, file_limit, size_limit, compression_ratio_limit):
        self.source_url = source_url
        self.work_dir = work_dir
        self.file_total = 0
        self.size_total = 0
        self.file_limit = file_limit
        self.size_limit = size_limit
        self.compression_ratio_limit = compression_ratio_limit


    def build(self) -> "EventLogBuilder":
        if self.source_url.scheme in {"https", "s3"}:
            local_path = self.work_dir.joinpath(self._basename())
            self._download(local_path)
        else:
            local_path = Path(self.source_url.path)

        event_logs = self._extract_archive(local_path, self.work_dir)

        # Remove the file if we downloaded it to free up space
        if self.source_url.scheme in {"https", "s3"}:
            local_path.unlink()

        self.event_log = self._concat(event_logs, self.work_dir.joinpath(local_path.name[:-len("".join(local_path.suffixes))] + "-concatenated.json"))

        return self


    def _extract_archive(self, event_log: Path, extract_dir: Path | None = None):
        if not extract_dir:
            extract_dir = event_log.parent

        extension = "".join(event_log.suffixes)
        if extension.endswith(".tar.gz"):
            return self._extract_tgz(event_log, extract_dir.joinpath(event_log.name[:-len(".tar.gz")]))
        if extension.endswith(".tgz"):
            return self._extract_tgz(event_log, extract_dir.joinpath(event_log.name[:-len(".tgz")]))
        if extension.endswith(".zip"):
            return self._extract_zip(event_log, extract_dir.joinpath(event_log.name[:-len(".zip")]))
        if extension.endswith(".gz"):
            return self._extract_gz(event_log, extract_dir.joinpath(event_log.name[:-len(".gz")]))
        if extension in {".json", ".log", ""}:
            return [event_log]

        raise ValueError("Unsupported extension found in the archive")


    def _concat(self, event_logs: list[Path], event_log: Path):
        if len(event_logs) == 1:
            return event_logs[0]

        dat = []
        for log in event_logs:
            with open(log) as log_file:
                try:
                    line = json.loads(log_file.readline())
                except ValueError:
                    continue # Maybe a Databricks pricing file
                if line['Event'] == "DBCEventLoggingListenerMetadata":
                    dat.append((line['Rollover Number'], line['SparkContext Id'], log))
                else:
                    raise ValueError("Expected DBC event not found")

        df = pd.DataFrame(dat, columns=["rollover_index", "context_id", "path"]).sort_values("rollover_index")
        
        if not len(df.context_id.unique()) == 1:
            raise ValueError("Not all rollover files have the same Spark context ID")

        diffs = df.rollover_index.diff()[1:]

        if any(diffs > 1):
            raise ValueError("Rollover file appears to be missing")

        if any(diffs < 1):
            raise ValueError("Duplicate rollover file detected")

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
                with tar_file.extractfile(tf.name) as source, open(extract_path, "wb") as target:
                    shutil.copyfileobj(source, target)

                # Remove the archive if it was extracted to free up space
                sub_paths = self._extract_archive(extract_path)
                if extract_path not in sub_paths:
                    self.file_total -= 1
                    self.size_total -= tf.size
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
                    self.file_total -= 1
                    self.size_total -= zinfo.file_size
                    extract_path.unlink()

                paths += sub_paths

        return paths


    def _extract_gz(self, archive_path: Path, extract_path: Path):
        with open(archive_path, "rb") as fobj:
            fobj.seek(-4, 2)
            size = struct.unpack("I", fobj.read(4))[0]

        self.file_total += 1
        self.size_total += size
        self._safety_check(size)

        with gzip.open(archive_path, "rb") as source, open(extract_path, "wb") as target:
            shutil.copyfileobj(source, target)

        # Remove the archive if it was extracted to free up space
        sub_paths = self._extract_archive(extract_path)
        if extract_path not in sub_paths:
            self.file_total -= 1
            self.size_total -= size
            extract_path.unlink()

        return sub_paths


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
            raise AssertionError("Encountered suspicious compression ratio in the archive")

        if self.size_total > self.size_limit:
            raise AssertionError("The archive is too big")

        if self.file_total > self.file_limit:
            raise AssertionError("Too many files in the archive")


    def _download(self, path):
        with open(path, 'wb') as fobj:
            if self.source_url.scheme == "https":
                response = requests.get(self.source_url, stream=True)
                for chunk in response.iter_content():
                        fobj.write(chunk)
            elif self.source_url.scheme == "s3":
                s3 = boto.client("s3")
                s3.download_fileobj(self.source_url.host, self.source_url.path, fobj)

        return path


    def _basename(self):
        if self.source_url.scheme in {"https", "s3"}:
            return self.source_url.path.split("/")[-1]
        elif self.source_url.scheme == "file":
            return Path(self.source_url.path).name

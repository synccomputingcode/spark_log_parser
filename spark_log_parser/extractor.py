import gzip
import os
import shutil
import struct
import tarfile
import zipfile
from pathlib import Path
from urllib.parse import ParseResult, urlparse

import requests
import boto3
from pydantic import BaseModel


class ExtractThresholds(BaseModel):
    entries = 100
    size = 5000000000
    ratio = 100


class Extractor:
    """
    Expands archive to a "work" directory and provides a list of extracted files
    """

    ALLOWED_SCHEMES = {"https", "s3", "file"}
    FILE_SKIP_PATTERNS = [".DS_Store".lower(), "__MACOSX".lower(), "/."]

    def __init__(
        self,
        source_url: ParseResult | str,
        work_dir: Path | str,
        s3_client=None,
        thresholds=ExtractThresholds(),
        # For data over https, default to loading 1MB chunks of the file at a time
        http_chunk_size=1024 * 1024,
    ):
        self.source_url = self._validate_url(source_url)
        self.work_dir = self._validate_work_dir(work_dir)
        self.s3_client = self._validate_s3_client(s3_client)
        self.file_total = 0
        self.size_total = 0

        self.thresholds = thresholds
        self.http_chunk_size = http_chunk_size

    def _validate_url(self, url: ParseResult | str) -> ParseResult:
        parsed_url = url if isinstance(url, ParseResult) else urlparse(url)
        if parsed_url.scheme not in self.ALLOWED_SCHEMES:
            raise ValueError(
                "URL scheme '%s' is not one of {'%s'}"
                % (parsed_url.scheme, "', '".join(self.ALLOWED_SCHEMES))
            )

        return parsed_url

    def _validate_work_dir(self, work_dir: Path | str) -> Path:
        work_dir_path = work_dir if isinstance(work_dir, Path) else Path(work_dir)
        if not work_dir_path.is_dir():
            raise ValueError("Path is not a directory")

        return work_dir_path

    def _validate_s3_client(self, s3_client):
        if self.source_url.scheme == "s3" and s3_client is None:
            return boto3.client("s3")

        return s3_client

    def extract(self) -> list[Path]:
        if self.source_url.scheme in {"https", "s3"}:
            self._download()
            local_path = self.work_dir
        else:
            local_path = Path(self.source_url.path)

        result_files = self._extract(local_path, self.work_dir)

        return result_files

    def _extract(self, event_log: Path, extract_dir: Path | None = None) -> list[Path]:
        """returns list of uncompressed files"""
        if not extract_dir:
            extract_dir = event_log.parent

        if event_log.is_dir():
            return self._extract_dir(event_log, extract_dir)

        extension = "".join(event_log.suffixes)
        if extension.endswith(".tar.gz"):
            return self._extract_tgz(
                event_log, extract_dir.joinpath(event_log.name[: -len(".tar.gz")])
            )
        if extension.endswith(".tgz"):
            return self._extract_tgz(
                event_log, extract_dir.joinpath(event_log.name[: -len(".tgz")])
            )
        if extension.endswith(".zip"):
            return self._extract_zip(
                event_log, extract_dir.joinpath(event_log.name[: -len(".zip")])
            )
        if extension.endswith(".gz"):
            return self._extract_gz(event_log, extract_dir.joinpath(event_log.name[: -len(".gz")]))

        if extension in {".json", ".log", ""}:
            return [event_log]

        raise ValueError("Unsupported extension found in the archive")

    def _extract_dir(self, log_dir: Path, extract_dir: Path) -> list[Path]:
        paths = []

        for root, _, files in os.walk(log_dir):
            for file in files:
                path = Path(root, file)
                if self._should_skip_file(file):
                    if path.is_relative_to(self.work_dir):
                        path.unlink()
                    continue

                self._add_to_stats_and_verify(path.stat().st_size)

                sub_paths = self._extract(path, Path(extract_dir, path.parent.relative_to(log_dir)))
                if path not in sub_paths:
                    self._remove_from_stats(path.stat().st_size)
                    # Remove the archive if it was extracted to free up space
                    if path.is_relative_to(self.work_dir):
                        path.unlink()

                paths += sub_paths

        return paths

    def _extract_tgz(self, archive_path: Path, extract_dir: Path) -> list[Path]:
        paths = []

        with tarfile.open(archive_path) as tar_file:
            for tf in tar_file:
                if self._should_skip_file(tf.name):
                    continue
                if tf.isdir():
                    continue

                self._add_to_stats_and_verify(tf.size)

                extract_path = extract_dir.joinpath(tf.name)
                extract_path.parent.mkdir(parents=True, exist_ok=True)
                with tar_file.extractfile(tf.name) as source, open(extract_path, "wb") as target:
                    shutil.copyfileobj(source, target)

                # Remove the archive if it was extracted to free up space
                sub_paths = self._extract(extract_path)
                if extract_path not in sub_paths:
                    self._remove_from_stats(tf.size)
                    extract_path.unlink()

                paths += sub_paths

        return paths

    def _extract_zip(self, archive_path: Path, extract_dir: Path) -> list[Path]:
        paths = []

        with zipfile.ZipFile(archive_path) as zfile:
            for zinfo in zfile.infolist():
                if self._should_skip_file(zinfo.filename):
                    continue
                if zinfo.is_dir():
                    continue

                self._add_to_stats_and_verify(zinfo.file_size)

                extract_path = extract_dir.joinpath(zinfo.filename)
                extract_path.parent.mkdir(parents=True, exist_ok=True)
                with zfile.open(zinfo.filename) as source, open(extract_path, "wb") as target:
                    shutil.copyfileobj(source, target)

                # Remove the archive if it was extracted to free up space
                sub_paths = self._extract(extract_path)
                if extract_path not in sub_paths:
                    self._remove_from_stats(zinfo.file_size)
                    extract_path.unlink()

                paths += sub_paths

        return paths

    def _extract_gz(self, archive_path: Path, extract_path: Path) -> list[Path]:
        with open(archive_path, "rb") as fobj:
            fobj.seek(-4, 2)
            size = struct.unpack("I", fobj.read(4))[0]

        self._add_to_stats_and_verify(size)

        extract_path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(archive_path, "rb") as source, open(extract_path, "wb") as target:
            shutil.copyfileobj(source, target)

        # Remove the archive if it was extracted to free up space
        sub_paths = self._extract(extract_path)
        if extract_path not in sub_paths:
            self._remove_from_stats(size)
            extract_path.unlink()

        return sub_paths

    def _should_skip_file(self, filename: str) -> bool:
        if filename.startswith("."):
            return True

        filename = filename.lower()
        for name in self.FILE_SKIP_PATTERNS:
            if name in filename:
                return True

        return False

    def _add_to_stats_and_verify(self, size, count=1):
        self.size_total += size
        self.file_total += count

        ratio = size / self.size_total
        if ratio > self.thresholds.ratio:
            raise AssertionError("Encountered suspicious compression ratio in the archive")

        if self.size_total > self.thresholds.size:
            raise AssertionError("The archive is too big")

        if self.file_total > self.thresholds.entries:
            raise AssertionError("Too many files in the archive")

    def _remove_from_stats(self, size, count=1):
        self.size_total -= size
        self.file_total -= count

    def _download(self):
        if self.source_url.scheme == "https":
            response = requests.get(self.source_url.geturl(), stream=True)
            response.raise_for_status()

            if not int(response.headers.get("Content-Length", 0)):
                raise AssertionError("Download is empty")

            target_path = self.work_dir.joinpath(self.source_url.path.split("/")[-1])
            with open(target_path, "wb") as fobj:
                for chunk in response.iter_content(chunk_size=self.http_chunk_size):
                    fobj.write(chunk)
        elif self.source_url.scheme == "s3":
            """
            s3 protocol formats look like -
                s3://some/path/to/file/file.ext
            
            When parsed with urllib.parse, the first part of the path will be parsed as the `netloc` (which is
            'some' from our above example). The rest of the path will show up under `path`. So when we call 
            s3_client.list_objects_v2(Bucket=netloc, Prefix=path), we are basically filtering sub-paths within
            that top-level bucket that match that path! TBD is there is a more efficient way to do this. 
            """
            source_bucket = self.source_url.netloc
            source_key = self.source_url.path.lstrip("/")

            s3_content_count = 0
            s3_content_size = 0
            result = self.s3_client.list_objects_v2(Bucket=source_bucket, Prefix=source_key)
            if "Contents" in result:
                relative_index = source_key.rfind("/") + 1
                for content in result["Contents"]:
                    s3_content_count += 1
                    s3_content_size += content["Size"]
                    if s3_content_count > self.thresholds.entries:
                        raise AssertionError("Too many objects at %s" % self.source_url)
                    if s3_content_size > self.thresholds.size:
                        raise AssertionError(
                            "Size limit exceeded while downloading from %s" % self.source_url
                        )

                    target_path = self.work_dir.joinpath(
                        *content["Key"][relative_index:].split("/")
                    )
                    target_path.parent.mkdir(parents=True, exist_ok=True)
                    self.s3_client.download_file(source_bucket, content["Key"], str(target_path))

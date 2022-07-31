import os
import boto3 as boto
import gzip
from pathlib import Path
import shutil
import struct
import tarfile
import zipfile

import requests


THRESHOLD_ENTRIES = 100
THRESHOLD_SIZE = 5000000000
THRESHOLD_RATIO = 100


class Extractor:
    """
    Expands archive to a "work" directory and provides a list of extracted files
    """

    def __init__(self, source_url, work_dir):
        self.source_url = source_url
        self.work_dir = work_dir
        self.file_total = 0
        self.size_total = 0

    def extract(self) -> list[Path]:
        if self.source_url.scheme in {"https", "s3"}:
            local_path = self.work_dir.joinpath(self.source_url.path.split("/")[-1])
            self._download(local_path)
        else:
            local_path = Path(self.source_url.path)

        files = self._extract(local_path, self.work_dir)

        # Remove the archive if we downloaded it to free up space
        if local_path not in files and self.source_url.scheme in {"https", "s3"}:
            local_path.unlink()

        return files

    def _extract(self, event_log: Path, extract_dir: Path | None = None) -> list[Path]:
        """ returns list of uncompressed files"""
        if not extract_dir:
            extract_dir = event_log.parent

        if event_log.is_dir():
            return self._extract_dir(event_log, extract_dir.joinpath(event_log.name))

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

        source_root = Path(self.source_url.path)

        for root, _, files in os.walk(log_dir):
            for file in files:
                if self._should_skip_file(file):
                    continue

                path = Path(root, file)

                self._add_to_stats_and_verify(path.stat().st_size)

                # Remove the archive if it was extracted to free up space
                sub_paths = self._extract(path, Path(extract_dir, path.parent.relative_to(source_root)))
                if path not in sub_paths:
                    self._remove_from_stats(path.stat().st_size)

                paths += sub_paths

        return paths


    def _extract_tgz(self, archive_path: Path, extract_dir: Path):
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

    def _extract_zip(self, archive_path: Path, extract_dir: Path):
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

    def _extract_gz(self, archive_path: Path, extract_path: Path):
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

    FILE_SKIP_PATTERNS = [".DS_Store".lower(), "__MACOSX".lower(), "/."]

    def _should_skip_file(self, filename: str):
        if filename.startswith("."):
            return True

        filename = filename.lower()
        for name in Extractor.FILE_SKIP_PATTERNS:
            if name in filename:
                return True

        return False

    def _add_to_stats_and_verify(self, size, count=1):
        self.size_total += size
        self.file_total += count

        ratio = size / self.size_total
        if ratio > THRESHOLD_RATIO:
            raise AssertionError("Encountered suspicious compression ratio in the archive")

        if self.size_total > THRESHOLD_SIZE:
            raise AssertionError("The archive is too big")

        if self.file_total > THRESHOLD_ENTRIES:
            raise AssertionError("Too many files in the archive")


    def _remove_from_stats(self, size, count=1):
        self.size_total -= size
        self.file_total -= count


    def _download(self, path):
        with open(path, "wb") as fobj:
            if self.source_url.scheme == "https":
                response = requests.get(self.source_url, stream=True)
                for chunk in response.iter_content():
                    fobj.write(chunk)
            elif self.source_url.scheme == "s3":
                s3 = boto.client("s3")
                s3.download_fileobj(self.source_url.host, self.source_url.path, fobj)

        return path
import abc
import gzip
import zipfile
import tarfile

from io import IOBase, BytesIO, RawIOBase
from pathlib import Path
from urllib.parse import ParseResult, urlparse

from aiodataloader import DataLoader

from spark_log_parser.loaders import AbstractFileDataLoader


class AbstractLocalFileDataLoader(AbstractFileDataLoader):
    """

    """

    @abc.abstractmethod
    def read_gz_file(self, file):
        yield from super().read_gz_file(file)

    @abc.abstractmethod
    def read_tgz_archive(self, archive: tarfile.TarFile):
        yield from super().read_tgz_archive(archive)

    @abc.abstractmethod
    def read_zip_archive(self, archive: zipfile.ZipFile):
        yield from super().read_zip_archive(archive)

    @abc.abstractmethod
    def read_uncompressed_file(self, file):
        yield from super().read_uncompressed_file(file)

    def extract_directory(self, dir: Path):
        for path in dir.iterdir():
            yield from self.extract(path)

    def load_item(self, filepath: str | ParseResult):
        parsed_url: ParseResult = filepath if isinstance(filepath, ParseResult) else urlparse(filepath)
        path: Path = Path(parsed_url.path)

        yield from self.extract(path)


class LocalFileBlobDataLoader(AbstractLocalFileDataLoader):
    def read_gz_file(self, file):
        yield from super().read_gz_file(file)

    def read_tgz_archive(self, archive):
        yield from super().read_tgz_archive(archive)

    def read_zip_archive(self, archive: zipfile.ZipFile):
        yield from super().read_zip_archive(archive)

    def read_uncompressed_file(self, file):
        yield from super().read_uncompressed_file(file)


class LocalFileLinesDataLoader(AbstractLocalFileDataLoader):
    def read_gz_file(self, file):
        while line := file.readline():
            yield line

    def read_tgz_archive(self, archive):
        yield from super().read_tgz_archive(archive)

    def read_zip_archive(self, archive: zipfile.ZipFile):
        yield from super().read_zip_archive(archive)

    def read_uncompressed_file(self, file):
        while line := file.readline():
            yield line

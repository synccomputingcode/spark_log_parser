import abc
import gzip
import zipfile
import tarfile

from io import IOBase, BytesIO, RawIOBase, BufferedIOBase
from pathlib import Path
from urllib.parse import ParseResult, urlparse

from aiodataloader import DataLoader


class AbstractFileDataLoader(abc.ABC, DataLoader):
    """

    """

    # TODO - this would probably be nice to have in some abstract class?
    FILE_SKIP_PATTERNS = [".DS_Store".lower(), "__MACOSX".lower(), "/."]

    def _should_skip_file(self, filename: str) -> bool:
        if filename.startswith("."):
            return True

        filename = filename.lower()
        for name in self.FILE_SKIP_PATTERNS:
            if name in filename:
                return True

        return False

    @abc.abstractmethod
    def read_gz_file(self, file):
        yield file.read()

    @abc.abstractmethod
    def read_tgz_archive(self, archive: tarfile.TarFile):
        # TODO - validate size
        size = 0
        for tarinfo in archive:
            if self._should_skip_file(tarinfo.name):
                continue
            if tarinfo.isdir():
                continue

            size += tarinfo.size

            file_bytes = archive.extractfile(tarinfo)
            yield from self.extract(Path(tarinfo.name), file_bytes)

    @abc.abstractmethod
    def read_zip_archive(self, archive: zipfile.ZipFile):
        # TODO - validate size
        size = 0
        for zinfo in archive.infolist():
            if self._should_skip_file(zinfo.filename):
                continue

            if zinfo.is_dir():
                continue

            size += zinfo.file_size

            # TODO - this archive.read(zinfo) reads the whole file into memory. Would be nice to stream it, instead
            file_bytes = BytesIO(archive.read(zinfo))
            yield from self.extract(Path(zinfo.filename), file_bytes)

    def extract_directory(self, dir: Path):
        for path in dir.iterdir():
            yield from self.extract(path)

    @abc.abstractmethod
    def read_uncompressed_file(self, file):
        return file.read()

    def extract(self, filepath: Path, file_bytes: BufferedIOBase = None):
        if file_bytes is None and filepath.is_dir():
            yield from self.extract_directory(filepath)

        to_open = file_bytes if file_bytes is not None else filepath

        match filepath.suffixes:
            case [".tgz"] | [".tar", ".gz"]:
                # print(f"Unpacking gzipped tarball: {to_open}")
                with tarfile.open(filepath, "r:gz", file_bytes) as tgz:
                    yield from self.read_tgz_archive(tgz)

            case [".zip"]:
                # print("Unpacking .zip archive")
                with zipfile.ZipFile(to_open) as archive:
                    yield from self.read_zip_archive(archive)

            case [*suffixes] if (len(suffixes) > 0 and suffixes[-1] == ".gz"):
                # print("Processing gzipped file")
                with gzip.open(to_open) as file:
                    yield from self.read_gz_file(file)

            # If the Path is not a directory and has no suffix, it will match []
            case [".json"] | [".log"] | []:
                # print("Yielding raw file")
                if file_bytes:
                    yield from self.read_uncompressed_file(file_bytes)
                else:
                    with open(to_open, "r", encoding="utf-8") as file:
                        yield from self.read_uncompressed_file(file)

            case _:
                # TODO - is this the correct Error type?
                raise ValueError(f"Unknown file format {''.join(filepath.suffixes)}")

    @abc.abstractmethod
    def load_item(self, filepath: str | ParseResult):
        """
        Since the concrete Loader may be fetching data from disparate data sources, this method should be
        responsible for grabbing the raw data stream for each file that may then be passed to extract().
        """
        pass

    async def batch_load_fn(self, keys: list[str]):
        return [self.load_item(filepath) for filepath in keys]

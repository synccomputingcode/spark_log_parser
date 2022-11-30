import abc
import gzip
import zipfile
import tarfile

from io import IOBase, BytesIO, BufferedIOBase
from pathlib import Path
from urllib.parse import ParseResult

from aiodataloader import DataLoader

FILE_SKIP_PATTERNS = [".DS_Store".lower(), "__MACOSX".lower(), "/."]


class AbstractFileDataLoader(abc.ABC, DataLoader):
    """

    """

    @staticmethod
    def should_skip_file(filename: str) -> bool:
        """
        Utility method for determining which files in directories / archives we should we skip (i.e. not parse/download)
        """
        if filename.startswith("."):
            return True

        filename = filename.lower()
        return any([name in filename for name in FILE_SKIP_PATTERNS])

    @abc.abstractmethod
    def read_tgz_archive(self, archive: tarfile.TarFile):
        """

        """
        # TODO - validate size
        size = 0
        for tarinfo in archive:
            if self.should_skip_file(tarinfo.name):
                continue
            if tarinfo.isdir():
                continue

            size += tarinfo.size

            file_bytes = archive.extractfile(tarinfo)
            yield from self.extract(Path(tarinfo.name), file_bytes)

    @abc.abstractmethod
    def read_zip_archive(self, archive: zipfile.ZipFile):
        """

        """
        # TODO - validate size
        size = 0
        for zinfo in archive.infolist():
            if self.should_skip_file(zinfo.filename):
                continue

            if zinfo.is_dir():
                continue

            size += zinfo.file_size

            # TODO - this archive.read(zinfo) reads the whole file into memory. Would be nice to stream it, instead
            file_bytes = BytesIO(archive.read(zinfo))
            yield from self.extract(Path(zinfo.filename), file_bytes)

    @abc.abstractmethod
    def read_gz_file(self, file, filepath=None):
        """

        """

    @abc.abstractmethod
    def read_uncompressed_file(self, file: IOBase):
        """

        """

    @abc.abstractmethod
    def load_item(self, filepath: str | ParseResult):
        """
        Since the concrete Loader may be fetching data from disparate data sources, this method should be
        responsible for grabbing the raw data stream for each file that may then be passed to extract().

        Alternatively, if the file is present locally on the machine, just passing a filepath to extract()
        will suffice
        """

    def extract_directory(self, directory: Path):
        for path in directory.iterdir():
            yield from self.extract(path)

    def extract(self, filepath: Path, file_bytes: BufferedIOBase = None):
        """

        """
        if file_bytes is None and filepath.is_dir():
            yield from self.extract_directory(filepath)

        to_open = file_bytes if file_bytes is not None else filepath

        match filepath.suffixes:
            case [".tgz"] | [".tar", ".gz"]:
                print(f"Unpacking gzipped tarball: {filepath}")
                with tarfile.open(filepath, "r:gz", file_bytes) as tgz:
                    yield from self.read_tgz_archive(tgz)

            case [".zip"]:
                print(f"Unpacking .zip archive: {filepath}")
                with zipfile.ZipFile(to_open) as archive:
                    yield from self.read_zip_archive(archive)

            case [*suffixes] if (len(suffixes) > 0 and suffixes[-1] == ".gz"):
                print(f"Processing gzipped file: {filepath}")
                with gzip.open(to_open) as file:
                    # We may see files like {name}.zip.gz, so make sure we handle that appropriately
                    if len(suffixes) == 1:
                        yield from self.read_gz_file(file)
                    else:
                        new_path = Path(str(filepath).removesuffix(".gz"))
                        yield from self.extract(new_path, file)

            # If the Path is not a directory and has no suffix, it will match []
            case [".json"] | [".log"] | []:
                print(f"Yielding raw file: {filepath}")
                if file_bytes:
                    yield from self.read_uncompressed_file(file_bytes)
                else:
                    with open(to_open, "rb", encoding="utf-8") as file:
                        yield from self.read_uncompressed_file(file)

            case _:
                # TODO - is this the correct Error type?
                raise ValueError(f"Unknown file format {''.join(filepath.suffixes)}")

    async def batch_load_fn(self, keys: list[str]):
        return [self.load_item(filepath) for filepath in keys]


class AbstractBlobDataLoader(AbstractFileDataLoader, abc.ABC):
    """
    Abstract class that implements the various read_* methods from AbstractFileDataLoader in such a way
    as to yield full "blobs" of data from the underlying file source. Useful for loading e.g. JSON files
    """
    def read_tgz_archive(self, archive):
        yield from super().read_tgz_archive(archive)

    def read_zip_archive(self, archive: zipfile.ZipFile):
        yield from super().read_zip_archive(archive)

    def read_gz_file(self, file):
        yield file.read()

    def read_uncompressed_file(self, file):
        yield file.read()


class AbstractLinesDataLoader(AbstractFileDataLoader, abc.ABC):
    """
    Abstract class that implements the various read_* methods from AbstractFileDataLoader in such a way
    as to yield individual lines of the underlying file source. Useful for loading e.g. JSON Lines or CSV files
    """

    def read_tgz_archive(self, archive):
        yield from super().read_tgz_archive(archive)

    def read_zip_archive(self, archive: zipfile.ZipFile):
        yield from super().read_zip_archive(archive)

    def read_gz_file(self, file):
        while line := file.readline():
            yield line

    def read_uncompressed_file(self, file):
        while line := file.readline():
            yield line

import abc
import gzip
import zipfile
import tarfile

from io import IOBase, BytesIO, BufferedIOBase
from pathlib import Path
from urllib.parse import ParseResult

from aiodataloader import DataLoader
from stream_unzip import stream_unzip

FILE_SKIP_PATTERNS = [".DS_Store".lower(), "__MACOSX".lower(), "/."]


class ZipArchiveMemberWrapper:
    """
    Minimal file-like wrapper to wrap the output of stream_unzip. This allows a streaming-compatible method of
    unzipping .zip archives, which zipfile.ZipFile does not support.

    Note - this wrapper does not implement the full file-like object API. Instead, it only implements at the moment
        those methods that are actually called when loading .zip archives in various contexts. This doesn't mean that
        new methods won't need to be implemented in the future as use-cases grow, it just means that at the moment
        this minimal implementation satisfies the current use-cases we do have

    Note #2 - once `chunks` is consumed, calls to read/lines will return empty byte buffers
    """

    _trailing_data: bytes = None

    def __init__(self, chunks):
        self.chunks = chunks

    def read(self, size=-1):
        # If we have leftover data from prior reads, use that. Otherwise, we assume that the underlying `chunk`s from
        # our zip archive stream are raw bytes
        content = self._trailing_data if self._trailing_data is not None else b''
        self._trailing_data = None

        if not size or size == -1:
            for chunk in self.chunks:
                content += chunk
        else:
            # iterate chunks until we've read at least `size` amount of data
            for chunk in self.chunks:
                content += chunk
                if len(content) >= size:
                    break

            # Trim off any data that is past the provided `size` and hold on to it for subsequent `read` calls
            if len(content) > size:
                self._trailing_data = content[size:]
                content = content[:size]

        return content

    def readlines(self):
        pending = None

        for chunk in self.chunks:

            if pending is not None:
                chunk = pending + chunk

            lines = chunk.splitlines()

            # Check if the last item in our last line is the same as the last item in our original un-split line.
            #  If it is, that means our original chunk did not end in a newline and we need to hang on to that data
            #  for processing the next chunk
            if lines and lines[-1] and chunk and lines[-1][-1] == chunk[-1]:
                pending = lines.pop()
            else:
                pending = None

            for line in lines:
                yield line

        if pending is not None:
            yield pending


class AbstractFileDataLoader(abc.ABC, DataLoader):
    """
    Base class for loading file over various transport mechanisms. This commits to streaming file data as much as
    possible, so that we are not holding on to full representations of large files in-memory. This means that any
    subclasses which are overriding any of the abstract methods of this class should conform to this commitment
    and make sure that their implementations are streaming-compatible as well. As a concrete example, any
    implementations of `self.load_item` that aren't dealing with files found locally on disk should fetch
    the file stream for the given `filepath`, and make sure to pass that stream to `self.extract`.

    Currently, this class supports the following file extensions -
        - .zip
        - .tgz / .tar.gz
        - .json
        - .log
        - .gz
        - no file extension
    As well as some combinations of those extensions, i.e. .json.gz for gzipped JSON files.
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

            # TODO - Size assertions?
            size += tarinfo.size

            file_bytes = archive.extractfile(tarinfo)
            yield from self.extract(Path(tarinfo.name), file_bytes)

    @abc.abstractmethod
    def read_zip_archive(self, raw_archive_stream):
        """

        """

        total_size = 0
        for fname, fsize, chunks in stream_unzip(raw_archive_stream):
            fname = fname.decode("utf-8")
            should_skip = fsize == 0 or self.should_skip_file(fname)
            if should_skip:
                # We still need to exhaust the chunks for this part of the archive in order to
                #  be able to consume the stream for the next part! If we don't do this, stream_unzip
                #  will raise an UnfinishedIterationError
                for _ in chunks:
                    continue
                continue

            # TODO - Size assertions?
            if fsize is not None:
                total_size += fsize

            wrapped = ZipArchiveMemberWrapper(chunks)
            yield from self.extract(Path(fname), wrapped)

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

    def extract(self, filepath: Path, file_stream: BufferedIOBase = None):
        """

        """
        if file_stream is None and filepath.is_dir():
            yield from self.extract_directory(filepath)

        to_open = file_stream if file_stream is not None else filepath

        match filepath.suffixes:
            case [".tgz"] | [".tar", ".gz"]:
                print(f"Unpacking gzipped tarball: {filepath}")
                with tarfile.open(filepath, "r:gz", file_stream) as tgz:
                    yield from self.read_tgz_archive(tgz)

            case [".zip"]:
                print(f"Unpacking .zip archive: {filepath}")
                if file_stream:
                    yield from self.read_zip_archive(file_stream)

                elif filepath and file_stream is None:
                    # We use `open` here instead of `zipfile.ZipFile` because `zipfile.ZipFile` does not support
                    # unzipping streams that are not seekable (as is the case when given an e.g. HTTP or S3 Stream).
                    # In order the match the behaviour when we are given a `file_stream` of the .zip archive (just
                    # above), we can `open` the given filepath instead in binary mode, and pass that binary stream
                    # in to `read_zip_archive` where we can then unzip the archive in a streaming manner
                    with open(filepath, "rb") as fobj:
                        file_stream = fobj
                        yield from self.read_zip_archive(file_stream)

            case [*suffixes] if (len(suffixes) > 0 and suffixes[-1] == ".gz"):
                print(f"Processing gzipped file: {filepath}")
                with gzip.open(to_open) as file:
                    # We may see files like {name}.zip.gz/.json.gz, so make sure we handle that appropriately
                    if len(suffixes) == 1:
                        yield from self.read_gz_file(file)
                    else:
                        new_path = Path(str(filepath).removesuffix(".gz"))
                        yield from self.extract(new_path, file)

            # If the Path is not a directory and has no suffix, it will match []
            case [".json"] | [".log"] | []:
                print(f"Yielding raw file: {filepath}")
                if file_stream:
                    yield from self.read_uncompressed_file(file_stream)
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
        yield from file.readlines()

    def read_uncompressed_file(self, file):
        yield from file.readlines()

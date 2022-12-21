import abc
import gzip
import tarfile
import logging

from io import IOBase, BufferedIOBase
from pathlib import Path
from urllib.parse import ParseResult

from aiodataloader import DataLoader
from stream_unzip import stream_unzip

FILE_SKIP_PATTERNS = [".DS_Store".lower(), "__MACOSX".lower(), "/."]

logger = logging.getLogger("Loaders")


class ArchiveExtractionThresholds:
    entries = 100
    size = 5000000000
    ratio = 100


class FileChunkStreamWrapper:
    """
    Minimal file-like wrapper to wrap the output of file streams that may be lacking methods like read/readlines (or in
    other words, file streams that are raw generators). This allows us to wrap things like the output of `stream_unzip`
    or botocore.StreamingBody.iter_lines() and pass those streams around as file-like objects.

    Note - this wrapper does not implement the full file-like object API. Instead, it only implements at the moment
        those methods that are actually called when loading files in various contexts. This doesn't mean that
        new methods won't need to be implemented in the future as use-cases grow, it just means that at the moment
        this minimal implementation satisfies the current use-cases we do have

    Note #2 - once `chunks` is consumed, calls to read/lines will return empty byte buffers
    """

    _trailing_data: bytes = b''
    _chunks = None

    def __init__(self, chunks):
        assert chunks
        self._chunks = chunks

    def __iter__(self):
        """
        The implementation of
        """
        return self.readlines()

    def read(self, size=-1):
        content = self._trailing_data
        self._trailing_data = b''

        if not size or size == -1:
            for chunk in self._chunks:
                content += chunk
        else:
            # iterate chunks until we've read at least `size` amount of data
            for chunk in self._chunks:
                content += chunk
                if len(content) >= size:
                    break

            # Trim off any data that is past the provided `size` and hold on to it for subsequent `read` calls
            if len(content) > size:
                self._trailing_data = content[size:]
                content = content[:size]

        return content

    def readlines(self):
        """
        Returns a stream of lines from self.chunks. This differs from many readlines() implementations, which tend to
        read the whole file into memory and return a list of the lines
        """
        pending = None

        for chunk in self._chunks:

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


class ZipArchiveMemberWrapper(FileChunkStreamWrapper):
    pass


class AbstractFileDataLoader(abc.ABC, DataLoader):
    """
    Base class for loading files over various transport mechanisms. This commits to streaming file data as much as
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

    cache = False
    logger = None

    _extraction_thresholds: ArchiveExtractionThresholds

    def __init__(self, extraction_thresholds: ArchiveExtractionThresholds = None):
        super().__init__()
        self._extraction_thresholds = extraction_thresholds or ArchiveExtractionThresholds()


    @staticmethod
    def should_skip_file(filename: str) -> bool:
        """
        Utility method for determining which files in directories / archives we should we skip (i.e. not parse/download)
        """
        if filename.startswith("."):
            return True

        filename = filename.lower()
        return any([name in filename for name in FILE_SKIP_PATTERNS])

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
                #  will raise an UnfinishedIterationError, since we can't just skip ahead
                for _ in chunks:
                    continue
                continue

            # TODO - Size assertions?
            if fsize is not None:
                total_size += fsize

            wrapped = ZipArchiveMemberWrapper(chunks)
            yield from self.extract(Path(fname), wrapped)

    @abc.abstractmethod
    def read_file_stream(self, file: IOBase):
        """
        When we finally hit a file that no longer needs to be expanded/decompressed in some manner, this method
        will be called to yield data from the file in some manner. This is here is order to allow concrete classes
        to decide how they actually want to yield data from the file, whether that be raw chunks, as lines, or a blob
        of the whole file (or something else entirely)
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
                self.logger.info(f"Unpacking gzipped tarball: {filepath}")
                # Note - `r|gz` means that the filepath is processed as a stream of blocks, and as such
                # does not allow seeking. See - https://docs.python.org/3.10/library/tarfile.html#tarfile.open
                with tarfile.open(filepath, "r|gz", file_stream) as tgz:
                    yield from self.read_tgz_archive(tgz)

            case [".zip"]:
                self.logger.info(f"Unpacking .zip archive: {filepath} from stream: {file_stream}")
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
                self.logger.info(f"Processing gzipped file: {filepath}")
                with gzip.open(to_open) as file:
                    # We may see files like {name}.zip.gz/.json.gz, so make sure we handle that appropriately
                    if len(suffixes) == 1:
                        yield from self.read_file_stream(file)
                    else:
                        new_path = Path(str(filepath).removesuffix(".gz"))
                        yield from self.extract(new_path, file)

            # If the Path is not a directory and has no suffix, it will match []
            case [".json"] | [".log"] | []:
                self.logger.info(f"Yielding raw file: {filepath}, from stream: {to_open}")
                if file_stream:
                    yield from self.read_file_stream(file_stream)
                else:
                    with open(to_open, "rb") as file:
                        yield from self.read_file_stream(file)

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

    cache = False

    def read_file_stream(self, file):
        yield file.read()


class AbstractLinesDataLoader(AbstractFileDataLoader, abc.ABC):
    """
    Abstract class that implements the various read_* methods from AbstractFileDataLoader in such a way
    as to yield individual lines of the underlying file source. Useful for loading e.g. JSON Lines or CSV files
    """

    cache = False

    def read_file_stream(self, file):
        for line in file:
            yield line

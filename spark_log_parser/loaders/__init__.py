import abc
import collections
import gzip
import tarfile
import logging

from dataclasses import dataclass
from io import BufferedIOBase
from pathlib import Path
from typing import TypeVar, Iterator
from urllib.parse import ParseResult

from aiodataloader import DataLoader
from stream_unzip import stream_unzip

logger = logging.getLogger("Loaders")

FILE_SKIP_PATTERNS = [".DS_Store".lower(), "__MACOSX".lower(), "/."]


# See the definition of `consume` here - https://docs.python.org/3/library/itertools.html#itertools-recipes
def exhaust_iterator(iterator: Iterator) -> None:
    collections.deque(iterator, maxlen=0)


@dataclass
class ArchiveExtractionThresholds:
    entries: int = 100
    size: int = 50000000000


class ArchiveTooLargeError(AssertionError):
    """Raised when we encounter an archive that expands to beyond the allowed filesize limit"""
    pass


class ArchiveTooManyEntriesError(AssertionError):
    """Raised when we encounter an archive that has more entries in it than is allowable"""
    pass


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

    def __init__(self, chunks: Iterator[bytes], maximum_file_size: int = ArchiveExtractionThresholds.size):
        assert chunks
        self.total_size: int = 0
        self._chunks: Iterator[bytes] = chunks
        self._maximum_allowed_size: int = maximum_file_size
        self._buffer: bytearray = bytearray()
        # Almost all the return values from self.read() will read out of this value instead of self._buffer.
        #  This is because reads from a memoryview are no-copy, whereas slices from a bytes/bytearray will copy
        #  the data. This means that if we fully read self._buffer, we would have fully duplicated the data at
        #  least one time. When dealing with file sizes that may be many 10s of GBs, this ends up resulting in a
        #  ton of memory overhead
        self._buffer_memview: memoryview | None = None
        self._buffer_position: int = 0  # Index of how far into _buffer/_buffer_memview we have read
        self._consumed: bool = False

    def __iter__(self):
        for chunk in self._chunks:
            if not chunk:
                continue
            else:
                self._add_chunk_to_size(chunk)
                yield chunk

    def _add_chunk_to_size(self, chunk) -> None:
        self.total_size += len(chunk)
        if self.total_size > self._maximum_allowed_size:
            raise ArchiveTooLargeError()

    def _num_bytes_left_in_buffer(self) -> int:
        return len(self._buffer) - self._buffer_position

    def _should_read_more(self, size) -> bool:
        return (not size or self._num_bytes_left_in_buffer() < size) and not self._consumed

    def read(self, size=-1) -> memoryview:
        if self._consumed:
            return memoryview(bytearray()).cast("B").toreadonly()

        if size < 0:
            size = None

        if self._should_read_more(size):
            # We take a slice of our buffer intentionally here. When a memoryview is held on a bytearray, resizing that
            #  bytearray is not allowed. However, if we take a slice of the data that we have not yet `read`, a copy of
            #  that data will be returned, and re-assigning self._buffer means that we are now free to append into it
            #  as necessary. Our old buffer reference/memoryview will now get GC'd at some point, which means that we
            #  don't need to manually release anything
            self._buffer = self._buffer[self._buffer_position:]
            self._buffer_position = 0

            while self._should_read_more(size):
                try:
                    chunk = next(self._chunks)
                    self._add_chunk_to_size(chunk)
                    self._buffer.extend(chunk)
                except StopIteration:
                    # Setting this to True will cause us to break out of this loop without needing an explicit `break`
                    self._consumed = True

            self._buffer_memview = memoryview(self._buffer).cast("B").toreadonly()

        start_pos = self._buffer_position
        self._buffer_position = self._buffer_position + size if (size and not self._consumed) else len(self._buffer)
        return self._buffer_memview[start_pos:self._buffer_position]

    _LINE_CHUNK_SIZE = 1024 * 1024

    def iter_lines(self) -> Iterator[bytes]:
        """
        Returns a stream of lines from self._chunks.
        """
        trailing_data = bytearray()
        # We use a relatively large chunk_size here to minimize the number of times we have to enter this loop/copy data
        while bs := self.read(self._LINE_CHUNK_SIZE):
            if trailing_data:
                # Instead of appending via e.g. bs = trailing_data + bs, which causes a ton of data copying,
                #  we can just write this chunk into our current line buffer. This is extremely helpful when we
                #  encounter large files where all the data is actually on one line (i.e. a 500MB JSON file that was
                #  written without any line breaks). This saves us 10s of seconds when processing these types of files
                # This is safe to do here because trailing_data is never referenced outside of this function, and we
                #  only ever return trailing_data when we are done reading in the whole file
                trailing_data.extend(bs)
                bs = trailing_data
                trailing_data = bytearray()
            else:
                # Since self.read() returns to us a memoryview, we need to copy this to a bytearray for splitlines()
                #  to work
                bs = bytearray(bs)

            # Don't check for `\r\n` because that will be caught by checking for '\n', so we only need to scan 1 time.
            #  Doing this similarly helps keep parsing fast for large single-line files, while having minimal
            #  performance impact on files that actually have more than 1 line
            lines = bs.splitlines() if b'\n' in bs else [bs]

            # This condition checks if the last byte in our last line is the same as the last byte of our chunk -
            #  if it is, then that means that our last line did not end in a newline, and therefore we need to hang
            #  on to that line for the next iteration.
            if lines and lines[-1] and bs and lines[-1][-1] == bs[-1]:
                # Just make sure that trailing_data is always a bytearray for future iterations
                trailing_data = lines.pop()
                if not isinstance(trailing_data, bytearray):
                    trailing_data = bytearray(trailing_data)

            if lines:
                yield from lines

        if trailing_data:
            yield trailing_data


class ZipArchiveMemberStreamWrapper(FileChunkStreamWrapper):
    """Just an alias for FileChunkStreamWrapper for use when extracting Zip archive members using stream-unzip"""


FileStreamIterator = TypeVar("FileStreamIterator", bound=Iterator[bytes])
FileExtractionResult = TypeVar("FileExtractionResult", bound=tuple[Path, FileStreamIterator])


class AbstractFileDataLoader(DataLoader, abc.ABC):
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

    def __init__(self, extraction_thresholds: ArchiveExtractionThresholds = None, **kwargs):
        super().__init__(**kwargs)
        self._extraction_thresholds: ArchiveExtractionThresholds = extraction_thresholds or ArchiveExtractionThresholds()

    @staticmethod
    def should_skip_file(filename: str) -> bool:
        """
        Utility method for determining which files in directories / archives we should we skip (i.e. not parse/download)
        """
        if filename.startswith("."):
            return True

        filename = filename.lower()
        return any([name in filename for name in FILE_SKIP_PATTERNS])

    def read_tgz_archive(self, archive: tarfile.TarFile) -> Iterator[FileExtractionResult]:
        """
        Utility for unpacking a tarball, asserting that the contents of that tarball fall within our file-size
        constraints
        """
        num_files = 0
        size_left = self._extraction_thresholds.size

        for tarinfo in archive:
            if self.should_skip_file(tarinfo.name):
                continue

            if tarinfo.isdir():
                continue

            num_files += 1
            if num_files > self._extraction_thresholds.entries:
                raise ArchiveTooManyEntriesError()

            wrapped_bytes = FileChunkStreamWrapper(archive.extractfile(tarinfo), size_left)
            yield from self.extract(Path(tarinfo.name), wrapped_bytes)

            size_left -= wrapped_bytes.total_size

    _ARCHIVE_CHUNK_SIZE = 1024 * 1024

    def read_zip_archive(self, raw_archive_stream) -> Iterator[FileExtractionResult]:
        """
        Utility for unpacking a zip archive, asserting that the contents of that archive fall within our file-size
        constraints
        """
        num_files = 0
        size_left = self._extraction_thresholds.size

        for fname, fsize, chunks in stream_unzip(raw_archive_stream, chunk_size=self._ARCHIVE_CHUNK_SIZE):
            num_files += 1
            if num_files > self._extraction_thresholds.entries:
                raise ArchiveTooManyEntriesError()

            fname = fname.decode("utf-8")
            # We can't just skip files where fsize is None, because at certain compression levels stream_unzip
            #  just returns None for all file sizes. This doesn't mean that there is no data in the file, just
            #  that the filesize is unknown.
            should_skip = fsize == 0 or self.should_skip_file(fname)
            if should_skip:
                # We still need to exhaust the chunks for this part of the archive in order to
                #  be able to consume the stream for the next part! If we don't do this, stream_unzip
                #  will raise an UnfinishedIterationError, since we can't just skip ahead
                for c in chunks:
                    size_left -= len(c)
                    if size_left <= 0:
                        raise ArchiveTooLargeError()
                continue

            wrapped_bytes = ZipArchiveMemberStreamWrapper(chunks, size_left)
            yield from self.extract(Path(fname), wrapped_bytes)
            exhaust_iterator(wrapped_bytes)

            # We read this from the member wrapper itself instead of trusting the `fsize` that is given to us from
            #  above because we do not control this archive, and as such, we need to treat those fsize numbers as
            #  untrusted/user input
            size_left -= wrapped_bytes.total_size

    def extract_directory(self, directory: Path) -> Iterator[FileExtractionResult]:
        for path in directory.iterdir():
            yield from self.extract(path)

    @staticmethod
    @abc.abstractmethod
    def read_file_stream(file: FileChunkStreamWrapper) -> FileStreamIterator:
        """
        This method will be called once we have reached a file that is fully uncompressed/untarred/etc.
        This is intended to allow concrete classes to have flexibility in the manner in which they yield data from the
        underlying file stream, whether that be raw chunks, lines, or as an entire blob (or something else entirely!)
        """

    def extract(self, filepath: Path, file_stream: BufferedIOBase = None) -> Iterator[FileExtractionResult]:
        """
        This method recursively extracts the contents of the file at the given filepath. If an open file_stream is
        also provided, that file_stream will be operated on instead.

        This method will ultimately return a sequence of tuples where the first item is the Path of the file that is
        currently open (if we are extracting an archive/directory, then this will be the name of the file within
        that location, not the name of the archive/directory itself), and the 2nd item is an Iterator of the bytes in
        that file.
        """
        if file_stream is None and filepath.is_dir():
            yield from self.extract_directory(filepath)

        match filepath.suffixes:
            case [".tgz"] | [".tar", ".gz"]:
                logger.info(f"Unpacking gzipped tarball: {filepath}")
                # Note - `r|gz` means that the filepath is processed as a stream of blocks, and as such
                # does not allow seeking. See - https://docs.python.org/3.10/library/tarfile.html#tarfile.open
                with tarfile.open(filepath, "r|gz", file_stream) as tgz:
                    yield from self.read_tgz_archive(tgz)

            case [".zip"]:
                logger.info(f"Unpacking .zip archive: {filepath} from stream: {file_stream}")
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
                logger.info(f"Processing gzipped file: {filepath}")
                to_open = file_stream if file_stream is not None else filepath
                with gzip.open(to_open) as file:
                    # We may see files like {name}.zip.gz/.json.gz, so make sure we handle that appropriately
                    if len(suffixes) == 1:
                        wrapped = FileChunkStreamWrapper(file)
                        yield filepath, self.read_file_stream(wrapped)
                    else:
                        new_path = Path(str(filepath).removesuffix(".gz"))
                        yield from self.extract(new_path, file)

            # If the Path is not a directory and has no suffix, it will match []
            case [".json"] | [".log"] | []:
                logger.info(f"Yielding raw file: {filepath}")
                match file_stream:
                    case None:
                        with open(filepath, "rb") as file:
                            wrapped = FileChunkStreamWrapper(file)
                            yield filepath, self.read_file_stream(wrapped)

                    case FileChunkStreamWrapper():
                        yield filepath, self.read_file_stream(file_stream)

                    case _:
                        wrapped = FileChunkStreamWrapper(file_stream)
                        yield filepath, self.read_file_stream(wrapped)

            case _:
                raise ValueError(f"Unknown file format {''.join(filepath.suffixes)}")

    @abc.abstractmethod
    def load_item(self, filepath: str | ParseResult) -> Iterator[FileExtractionResult]:
        """
        Since the concrete Loader may be fetching data from disparate data sources, this method should be
        responsible for grabbing the raw data stream for each file that may then be passed to extract().

        Alternatively, if the file is present locally on the machine, just passing a filepath to extract()
        will suffice
        """

    async def batch_load_fn(self, keys: list[str]) -> list[Iterator[FileExtractionResult]]:
        return [self.load_item(filepath) for filepath in keys]


class LinesFileReaderMixin:
    """
    Mixin that provides an implementation of `read_file_stream` for implementors of AbstractFileDataLoader that yields
    data from the underlying file as well-formed lines. Useful for parsing e.g. JSON Lines or CSV files.
    """

    @staticmethod
    def read_file_stream(file: FileChunkStreamWrapper) -> FileStreamIterator:
        yield from file.iter_lines()


class BlobFileReaderMixin:
    """
    Mixin that provides an implementation of `read_file_stream` for implementors of AbstractFileDataLoader that yields
    data from the underlying file as a singular blob. Useful for parsing e.g. JSON files.
    """

    @staticmethod
    def read_file_stream(file: FileChunkStreamWrapper) -> FileStreamIterator:
        yield file.read()

import abc
import gzip
import tarfile
import logging

from io import BufferedIOBase
from pathlib import Path
from typing import Iterator
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

    total_size: int = 0

    _maximum_allowed_size: ArchiveExtractionThresholds
    _trailing_data: bytes = b''
    _chunks: Iterator[bytes] = None

    def __init__(self, chunks: Iterator[bytes], maximum_file_size: int = None):
        assert chunks
        self._chunks = chunks
        self._maximum_allowed_size = maximum_file_size

    def __iter__(self):
        for chunk in self._chunks:
            self._add_chunk_to_size(chunk)
            yield chunk

    def _add_chunk_to_size(self, chunk):
        self.total_size += len(chunk)
        if self._maximum_allowed_size is not None and self.total_size > self._maximum_allowed_size:
            raise AssertionError("This archive is too big")

    def read(self, size=-1) -> bytes:
        content = self._trailing_data
        self._trailing_data = b''

        if not size or size == -1:
            for chunk in self._chunks:
                self._add_chunk_to_size(chunk)
                content += chunk
        else:
            # iterate chunks until we've read at least `size` amount of data
            while len(content) <= size:
                chunk = next(self._chunks)
                self._add_chunk_to_size(chunk)
                content += chunk

            # Trim off any data that is past the provided `size` and hold on to it for subsequent `read` calls
            if len(content) > size:
                self._trailing_data = content[size:]
                content = content[:size]

        return content

    def iter_lines(self) -> Iterator[bytes]:
        """
        Returns a stream of lines from self._chunks.
        """
        for chunk in self._chunks:
            self._add_chunk_to_size(chunk)
            if self._trailing_data:
                chunk = self._trailing_data + chunk

            lines = chunk.splitlines()

            # This condition checks if the last byte in our last line is the same as the last byte of our chunk -
            #  if it is, then that means that our last line did not end in a newline, and therefore we need to hang
            #  on to that line for the next iteration.
            if lines and lines[-1] and chunk and lines[-1][-1] == chunk[-1]:
                self._trailing_data = lines.pop()
            else:
                self._trailing_data = b''

            for line in lines:
                yield line

        if self._trailing_data:
            yield self._trailing_data


class ZipArchiveMemberStreamWrapper(FileChunkStreamWrapper):
    """Just an alias for FileChunkStreamWrapper for use when extracting Zip archive members using stream-unzip"""

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

    def read_tgz_archive(self, archive: tarfile.TarFile)  -> Iterator[bytes]:
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
                raise AssertionError("Too many files present in this archive.")

            wrapped_bytes = FileChunkStreamWrapper(archive.extractfile(tarinfo), size_left)
            yield from self.extract(Path(tarinfo.name), wrapped_bytes)

            size_left -= wrapped_bytes.total_size

    def read_zip_archive(self, raw_archive_stream) -> Iterator[bytes]:
        """
        Utility for unpacking a zip archive, asserting that the contents of that archive fall within our file-size
        constraints
        """
        num_files = 0
        size_left = self._extraction_thresholds.size

        for fname, fsize, chunks in stream_unzip(raw_archive_stream):
            num_files += 1
            if num_files > self._extraction_thresholds.entries:
                raise AssertionError("Too many files present in this archive.")

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
                        raise AssertionError("This archive is too big")
                    continue
                continue

            wrapped_bytes = ZipArchiveMemberStreamWrapper(chunks, size_left)
            yield from self.extract(Path(fname), wrapped_bytes)

            # We read this from the member wrapper itself instead of trusting the `fsize` that is given to us from
            #  above because we do not control this archive, and as such, we need to treat those fsize numbers as
            #  untrusted/user input
            size_left -= wrapped_bytes.total_size

    def extract_directory(self, directory: Path) -> Iterator[bytes]:
        for path in directory.iterdir():
            yield from self.extract(path)

    def extract(self, filepath: Path, file_stream: BufferedIOBase = None) -> Iterator[bytes]:
        """
        This method takes a filepath and potentially an already-open file_stream (if the filepath cannot be found
        locally, the file_stream *must* be provided). Returns an Iterator of the underlying bytes of the file that
        is fully decompressed/unzipped/untarred/etc., and ready to be parsed into some useful representation.

        For archives, this Iterator will contain the bytes of all files present, except those that may be skipped (as
        defined in should_skip_file()).
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
                        yield from self.read_file_stream(file)
                    else:
                        new_path = Path(str(filepath).removesuffix(".gz"))
                        yield from self.extract(new_path, file)

            # If the Path is not a directory and has no suffix, it will match []
            case [".json"] | [".log"] | []:
                logger.info(f"Yielding raw file: {filepath}")
                if file_stream:
                    yield from self.read_file_stream(file_stream)
                else:
                    with open(to_open, "rb") as file:
                        yield from self.read_file_stream(file)

            case _:
                raise ValueError(f"Unknown file format {''.join(filepath.suffixes)}")

    @abc.abstractmethod
    def load_item(self, filepath: str | ParseResult):
        """
        Since the concrete Loader may be fetching data from disparate data sources, this method should be
        responsible for grabbing the raw data stream for each file that may then be passed to extract().

        Alternatively, if the file is present locally on the machine, just passing a filepath to extract()
        will suffice
        """

    async def batch_load_fn(self, keys: list[str]):
        return [self.load_item(filepath) for filepath in keys]

import abc
import boto3

from pathlib import Path
from urllib.parse import ParseResult, urlparse

from botocore.response import StreamingBody

from spark_log_parser.loaders import AbstractFileDataLoader, AbstractBlobDataLoader, AbstractLinesDataLoader, \
    FileChunkStreamWrapper


class S3StreamingBodyFileWrapper(FileChunkStreamWrapper):
    """
    Small wrapper around botocore.StreamingBody that exposes a lines iterator, in keeping with "pythonic" conventions.
    botocore.StreamingBody by default iterates over raw chunks of the input stream vs. in a line-delimited manner. This
    makes some sense, in general, since large files may not have newlines to split on. But as we expect to largely be
    dealing with line-delimited eventlog files, this default makes more sense for us to use.
    """

    def __init__(self, body: StreamingBody):
        # Since this stream is intended to be handed off to other classes for manipulation (i.e. unzipping), we set
        # keepends=True because various decoding libraries may throw decode errors if we remove parts of the binary
        # data stream
        #
        # Note - We need to watch out for this, though, as this means that iterating lines will give us binary buffers
        #  with `\n` at the end still. So if we are fetching an uncompressed file from S3, whatever is parsing those
        #  lines may need to be tolerant of that, OR we need to configure this flag properly. This would be pretty
        #  do-able by inspecting file extensions in AbstractS3FileDataLoader.load_item, but is not currently done.
        #  Right now, we may hit this case when loading raw .json files from S3, but json.loads(line) is tolerant of
        #  trailing newline characters. test_parse_s3.test_raw_log_from_s3 exercises this code path for raw .json files
        self._chunks = body.iter_lines(chunk_size=1024 * 1024, keepends=True)

    def __iter__(self):
        yield from self._chunks


class AbstractS3FileDataLoader(AbstractFileDataLoader, abc.ABC):
    """
    Abstract class that supports loading files directly from S3
    """

    _s3 = None

    def __init__(self):
        super().__init__()

    @property
    def s3(self):
        if self._s3 is None:
            self._s3 = boto3.client("s3")

        return self._s3

    def load_item(self, filepath):
        """

        """
        parsed_url: ParseResult = filepath if isinstance(filepath, ParseResult) else urlparse(filepath)
        bucket = parsed_url.netloc
        key = parsed_url.path.lstrip('/')

        object_list = self.s3.list_objects_v2(Bucket=bucket, Prefix=key)

        if object_list.get("IsTruncated", False):
            raise AssertionError(f"Too many objects at {filepath}.")

        contents_to_fetch = [content for content in object_list.get("Contents") if
                             not self.should_skip_file(content["Key"])]
        if not contents_to_fetch:
            raise AssertionError(f"No valid objects matching '{key}' in bucket: {bucket}")

        num_objects = len(contents_to_fetch)
        # TODO - define struct for these limits/thresholds
        if num_objects > 100:
            raise AssertionError(f"Too many objects in bucket: {bucket}.")

        total_size = 0
        for content in contents_to_fetch:
            total_size += content["Size"]
            if total_size > 20000000000:
                raise AssertionError(f"Size limit exceeded while downloading from {filepath}.")

        file_streams = []
        # TODO - this serially fetches all matching objects in the bucket at the moment...
        #  There is likely a better way to do this? It may require some ThreadExecutors, though
        for content in contents_to_fetch:
            # Wrap the botocore.response.StreamingBody and return that so that subsequent extraction can operate on the
            #  stream vs. loading all the files into memory
            data = self.s3.get_object(Bucket=bucket, Key=content["Key"])["Body"]
            wrapped = S3StreamingBodyFileWrapper(data)
            file_streams.append(wrapped)

        for (content, filestream) in zip(contents_to_fetch, file_streams):
            yield from self.extract(Path(content["Key"]), filestream)


class S3FileBlobDataLoader(AbstractS3FileDataLoader, AbstractBlobDataLoader):
    pass


class S3FileLinesDataLoader(AbstractS3FileDataLoader, AbstractLinesDataLoader):
    pass

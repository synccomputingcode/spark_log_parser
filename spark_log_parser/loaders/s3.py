import abc
import boto3

from pathlib import Path
from urllib.parse import ParseResult, urlparse

from botocore.response import StreamingBody

from spark_log_parser.loaders import AbstractFileDataLoader, BlobFileReaderMixin, LinesFileReaderMixin, \
    FileChunkStreamWrapper


class S3StreamingBodyFileWrapper(FileChunkStreamWrapper):
    """
    Small wrapper around botocore.StreamingBody that exposes a lines iterator, in keeping with "pythonic" conventions.
    botocore.StreamingBody by default iterates over raw chunks of the input stream vs. in a line-delimited manner. This
    makes some sense, in general, since large files may not have newlines to split on. But as we expect to largely be
    dealing with line-delimited eventlog files, this default makes more sense for us to use.
    """

    def __init__(self, body: StreamingBody, **kwargs):
        super().__init__(**kwargs, chunks=body.iter_chunks(1024 * 1024))


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

        contents_to_fetch = [content for content in object_list.get("Contents") if
                             not self.should_skip_file(content["Key"])]
        if not contents_to_fetch:
            raise AssertionError(f"No valid objects matching '{key}' in bucket: {bucket}")

        # TODO - define struct for these limits/thresholds
        if object_list.get("IsTruncated", False) or len(contents_to_fetch) > 100:
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


class S3FileBlobDataLoader(AbstractS3FileDataLoader, BlobFileReaderMixin):
    """
    Simple HTTP loader that returns the full file as a blob of data.
    """


class S3FileLinesDataLoader(AbstractS3FileDataLoader, LinesFileReaderMixin):
    """
    Simple HTTP loader that returns the file as a stream of lines (delimited by `\n`).
    """

import abc
import tarfile
import zipfile
import gzip
import boto3

from pathlib import Path
from urllib.parse import ParseResult, urlparse

from spark_log_parser.loaders import AbstractFileDataLoader, AbstractBlobDataLoader, AbstractLinesDataLoader


class AbstractS3FileDataLoader(AbstractFileDataLoader, abc.ABC):
    """
    Abstract class that supports loading files directly from S3
    """

    _s3 = None

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

        to_fetch = []
        total_size = 0

        if contents := object_list.get("Contents"):
            num_objects = len(contents)

            # TODO - define struct for these limits/thresholds
            if num_objects > 100:
                raise AssertionError(f"Too many objects at {filepath}.")

            for content in contents:
                total_size += content["Size"]
                if total_size > 20000000000:
                    raise AssertionError(f"Size limit exceeded while downloading from {filepath}.")

                filename = content["Key"]
                if not self.should_skip_file(filename):
                    to_fetch.append(filename)
        else:
            raise AssertionError(f"No objects matching '{key}' in bucket: {bucket}")

        files = []
        # TODO - this serially fetches all matching objects in the directory at the moment.
        #  There is likely a better way to do this!
        for filename in to_fetch:
            # Just append the botocore.response.StreamingBody so that extraction functions can operate on the stream
            # vs. loading all the files into memory
            data = self.s3.get_object(Bucket=bucket, Key=filename)["Body"]
            files.append(data)

        for (filepath, filestream) in zip(to_fetch, files):
            yield from self.extract(Path(filepath), filestream)


class S3FileBlobDataLoader(AbstractS3FileDataLoader, AbstractBlobDataLoader):
    pass


class S3FileLinesDataLoader(AbstractS3FileDataLoader, AbstractLinesDataLoader):
    pass

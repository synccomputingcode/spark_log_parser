import abc
import tarfile
import zipfile
import gzip
import boto3

from pathlib import Path
from urllib.parse import ParseResult, urlparse

from spark_log_parser.loaders import AbstractFileDataLoader


class AbstractS3FileDataLoader(AbstractFileDataLoader):
    """

    """

    _s3 = None

    @property
    def s3(self):
        if self._s3 is None:
            self._s3 = boto3.resource("s3")

        return self._s3

    @abc.abstractmethod
    def read_uncompressed_file(self, data):
        return data.decode("ascii")

    @abc.abstractmethod
    def read_gz_file(self, data):
        uncompressed = gzip.decompress(data)
        return uncompressed.decode("utf-8")

    @abc.abstractmethod
    def read_tgz_archive(self, archive: tarfile.TarFile):
        pass

    @abc.abstractmethod
    def read_zip_archive(self, archive: zipfile.ZipFile):
        pass

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
                if not self._should_skip_file(filename):
                    to_fetch.append(filename)
        else:
            raise AssertionError(f"No objects matching '{key}' in bucket: {bucket}")

        files = []
        # TODO - this serially fetches all matching objects in the directory at the moment.
        #  There is likely a better way to do this!
        for filename in to_fetch:
            # Just append the botocore.response.StreamingBody so that extraction functions can operate on the stream
            # vs. loading all the files into memory
            data = self.s3.Object(bucket, filename).get()["Body"]
            files.append(data)

        if len(files) > 1:
            for (filepath, filestream) in zip(to_fetch, files):
                yield from self.extract(filepath, filestream)
            # pass
        else:
            filepath = Path(to_fetch[0])
            filestream = files[0]
            yield from self.extract(filepath, filestream)

        # return files[0] if len(files) > 1 else files
        # return files


class S3FileBlobDataLoader(AbstractS3FileDataLoader):
    def read_uncompressed_data(self, data):
        return super().read_uncompressed_data(data)

    def read_gzipped_data(self, data):
        return super().read_gzipped_data(data)


class S3FileLinesDataLoader(AbstractS3FileDataLoader):
    def read_uncompressed_data(self, data):
        filestring = data.decode("utf-8")
        return filestring.splitlines(True)

    def read_gzipped_data(self, data):
        filestring = gzip.decompress(data).decode("utf-8")
        return filestring.splitlines(True)

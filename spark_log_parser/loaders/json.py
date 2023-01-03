import logging
import orjson
from typing import TypeVar

from aiodataloader import DataLoader

from spark_log_parser.loaders.https import HTTPFileLinesDataLoader, HTTPFileBlobDataLoader
from spark_log_parser.loaders.local_file import LocalFileBlobDataLoader, LocalFileLinesDataLoader
from spark_log_parser.loaders.s3 import S3FileBlobDataLoader, S3FileLinesDataLoader

RawJSONBlobDataLoader = TypeVar("BlobDataLoader", LocalFileBlobDataLoader, S3FileBlobDataLoader, HTTPFileBlobDataLoader)

logger = logging.getLogger("JSONLoaders")


class JSONBlobDataLoader(DataLoader):
    cache = False
    blob_data_loader: RawJSONBlobDataLoader

    def __init__(self, blob_data_loader: RawJSONBlobDataLoader, **kwargs):
        super().__init__(**kwargs)
        self.blob_data_loader = blob_data_loader

    @staticmethod
    def _yield_json(data):
        _, blob = data
        return orjson.loads(next(blob))

    async def batch_load_fn(self, keys):
        raw_datas = await self.blob_data_loader.load_many(keys)
        # We expect each "blob" here to be well-formed JSON, so parse each of them thusly
        return [self._yield_json(next(raw_data)) for raw_data in raw_datas]


RawJSONLinesDataLoader = TypeVar("LinesDataLoader", LocalFileLinesDataLoader, S3FileLinesDataLoader,
                                 HTTPFileLinesDataLoader)


class JSONLinesDataLoader(DataLoader):
    cache = False
    lines_data_loader: RawJSONLinesDataLoader

    @staticmethod
    def _yield_json_lines(data):
        for filepath, file in data:
            logger.info(f"Processing: {filepath}")

            lines = iter(file)
            first_line = next(lines)
            try:
                json_line = orjson.loads(first_line)
                yield json_line
            except orjson.JSONDecodeError:
                # If the first line is not parseable as a JSON object, try to parse the whole file as a JSON Object
                #  as well, and yield that as a line instead.
                # We do this for a few reasons -
                #  - Sometimes regular JSON files are delivered in archives along with eventlog files (which are JSON
                #     Lines). These JSON files may contain valuable information that we don't want to drop on the floor
                #     (for example, we may receive Databricks pricing information, which we may be able to use!)
                #  - We may not be able to tell without opening the file whether the eventlog provided to us is a "raw"
                #     eventlog (i.e. delivered to us in exactly the way Spark output them), or if it is an
                #     already-parsed log.
                #
                # This means that, in a sense, we treat regular JSON files as a special case of JSON Lines files
                for line in lines:
                    first_line += line

                try:
                    yield orjson.loads(first_line)
                except orjson.JSONDecodeError:
                    logger.warning(f"Could not parse file {filepath} as JSON - skipping")
                finally:
                    continue

            for line in lines:
                yield orjson.loads(line)

    def __init__(self, lines_data_loader: RawJSONLinesDataLoader, **kwargs):
        super().__init__(**kwargs)
        self.lines_data_loader = lines_data_loader

    async def batch_load_fn(self, keys):
        raw_datas = await self.lines_data_loader.load_many(keys)
        return [self._yield_json_lines(data) for data in raw_datas]

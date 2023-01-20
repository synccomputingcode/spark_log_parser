import logging
import time

import orjson
import ujson
from typing import TypeVar, Iterator

from aiodataloader import DataLoader

from spark_log_parser.loaders import FileExtractionResult
from spark_log_parser.loaders.https import HTTPFileLinesDataLoader, HTTPFileBlobDataLoader
from spark_log_parser.loaders.local_file import LocalFileBlobDataLoader, LocalFileLinesDataLoader
from spark_log_parser.loaders.s3 import S3FileBlobDataLoader, S3FileLinesDataLoader

RawJSONBlobDataLoader = TypeVar("BlobDataLoader", LocalFileBlobDataLoader, S3FileBlobDataLoader, HTTPFileBlobDataLoader)

logger = logging.getLogger("JSONLoaders")


class JSONBlobDataLoader(DataLoader):
    cache = False

    def __init__(self, blob_data_loader: RawJSONBlobDataLoader, **kwargs):
        super().__init__(**kwargs)
        self.blob_data_loader: RawJSONBlobDataLoader = blob_data_loader

    @staticmethod
    def _parse_as_json(data: FileExtractionResult):
        _, blob = data
        return orjson.loads(next(blob))

    async def batch_load_fn(self, keys):
        raw_datas = await self.blob_data_loader.load_many(keys)
        # We expect each "blob" here to be well-formed JSON, so parse each of them thusly
        return [self._parse_as_json(next(raw_data)) for raw_data in raw_datas]


RawJSONLinesDataLoader = TypeVar("LinesDataLoader", LocalFileLinesDataLoader, S3FileLinesDataLoader,
                                 HTTPFileLinesDataLoader)


class JSONLinesDataLoader(DataLoader):
    cache = False

    def __init__(self, lines_data_loader: RawJSONLinesDataLoader, **kwargs):
        super().__init__(**kwargs)
        self.lines_data_loader: RawJSONLinesDataLoader = lines_data_loader

    @staticmethod
    def _yield_json_lines(data: Iterator[FileExtractionResult]) -> Iterator[dict]:
        for filepath, file_stream in data:
            logger.info(f"Processing: {filepath}")

            lines = iter(file_stream)
            first_line = next(lines)
            try:
                json_line = orjson.loads(first_line)
                yield json_line

                # If we were able to successfully parse the first line as a JSON object, then we should be able
                #  to assume the rest of the lines in the file are well-formed JSON objects
                for line in lines:
                    yield orjson.loads(line)
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
                    # ujson is a little more lenient than orjson. In the interest of backwards compatibility, since
                    #  we used to write out parsed logs that could contain invalid JSON values like NaN (which orjson
                    #  does not support), we can try parsing this with data with a more lenient parser.
                    try:
                        yield ujson.loads(first_line)
                        # Log a warning when this happens so that we can hopefully have some indication of when it's OK
                        #  to remove this logic
                        logger.warning(f"Was able to parse file {filepath} with ujson but not orjson")
                    except ujson.JSONDecodeError:
                        logger.warning(f"Could not parse file {filepath} as JSON - skipping")

    async def batch_load_fn(self, keys):
        raw_datas = await self.lines_data_loader.load_many(keys)
        return [self._yield_json_lines(data) for data in raw_datas]

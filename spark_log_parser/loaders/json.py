import logging
import ujson as json
from typing import Generic, TypeVar

from aiodataloader import DataLoader

from spark_log_parser.loaders.https import HTTPFileLinesDataLoader, HTTPFileBlobDataLoader
from spark_log_parser.loaders.local_file import LocalFileBlobDataLoader, LocalFileLinesDataLoader
from spark_log_parser.loaders.s3 import S3FileBlobDataLoader, S3FileLinesDataLoader


RawJSONBlobDataLoader = TypeVar("BlobDataLoader", LocalFileBlobDataLoader, S3FileBlobDataLoader, HTTPFileBlobDataLoader)


class JSONBlobDataLoader(DataLoader, Generic[RawJSONBlobDataLoader]):
    blob_data_loader: RawJSONBlobDataLoader

    def __init__(self, blob_data_loader: RawJSONBlobDataLoader, **kwargs):
        super().__init__(**kwargs)
        self.blob_data_loader = blob_data_loader

    async def batch_load_fn(self, keys):
        raw_datas = await self.blob_data_loader.load_many(keys)
        # We expect each "blob" here to be well-formed JSON, so parse each of them thusly
        return [json.loads(next(raw_data)) for raw_data in raw_datas]


RawJSONLinesDataLoader = TypeVar("LinesDataLoader", LocalFileLinesDataLoader, S3FileLinesDataLoader,
                                 HTTPFileLinesDataLoader)


class JSONLinesDataLoader(DataLoader, Generic[RawJSONLinesDataLoader]):
    cache = False
    lines_data_loader: RawJSONLinesDataLoader

    def yield_json_lines(self, filename, lines):
        num_bad_lines_seen = 0
        for line in lines:
            try:
                yield json.loads(line)
            # TODO - this is largely here because we may get arbitrary file types in archives where we are
            #  expecting eventlog files (which are JSON Lines). If we try to load those "lines" as
            #  JSON objects, they will fail, and so this allows us to just skip those "bad" lines and
            #  continue processing other files in the archive. However, this `except` should probably
            #  be more robust so that we aren't ignoring real issues!
            except json.JSONDecodeError:
                num_bad_lines_seen += 1
                # We have to continue here because these will be the lines for the top-level file. If this is an
                #  archive, then it is possible for some lines to not be parse-able as JSON, but for others to be
                continue

        if num_bad_lines_seen > 0:
            self.logger.info(f"Failed to parse {num_bad_lines_seen} JSON lines in top-level file: {filename}")

    def __init__(self, lines_data_loader: RawJSONLinesDataLoader, **kwargs):
        super().__init__(**kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.lines_data_loader = lines_data_loader

    async def batch_load_fn(self, keys):
        raw_datas = await self.lines_data_loader.load_many(keys)
        return [self.yield_json_lines(key, data) for (key, data) in zip(keys, raw_datas)]

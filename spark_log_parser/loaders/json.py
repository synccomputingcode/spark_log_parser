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


def yield_json_lines(lines):
    for line in lines:
        try:
            yield json.loads(line)
        # TODO - this is largely here because we may get arbitrary file types in archives where we are
        #  expecting eventlog files (which are JSON Lines). If we try to load those "lines" as
        #  JSON objects, they will fail, and so this allows us to just skip those "bad" lines and
        #  continue processing other files in the archive. However, this `except` should probably
        #  be more robust so that we aren't ignoring real issues!
        except json.JSONDecodeError:
            # TODO - real logger
            print(line)
            # We have to continue here because these will be the lines for the top-level file. If this is an archive,
            #  then it is possible for some lines to not be parse-able as JSON, but for others to be
            continue


class JSONLinesDataLoader(DataLoader, Generic[RawJSONLinesDataLoader]):
    cache = False
    lines_data_loader: RawJSONLinesDataLoader

    def __init__(self, lines_data_loader: RawJSONLinesDataLoader, **kwargs):
        super().__init__(**kwargs)
        self.lines_data_loader = lines_data_loader

    async def batch_load_fn(self, keys):
        raw_datas = await self.lines_data_loader.load_many(keys)
        return [yield_json_lines(data) for data in raw_datas]

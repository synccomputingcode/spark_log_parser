import json
from typing import Generic, TypeVar

from aiodataloader import DataLoader

from spark_log_parser.loaders.https import HTTPStreamingDataLoader
from spark_log_parser.loaders.local_file import LocalFileBlobDataLoader, LocalFileLinesDataLoader
from spark_log_parser.loaders.s3 import S3FileBlobDataLoader, S3FileLinesDataLoader


RawJSONBlobDataLoader = TypeVar("BlobDataLoader", LocalFileBlobDataLoader, S3FileBlobDataLoader)


class JSONBlobDataLoader(DataLoader, Generic[RawJSONBlobDataLoader]):
    blob_data_loader: RawJSONBlobDataLoader

    def __init__(self, blob_data_loader: RawJSONBlobDataLoader, **kwargs):
        super().__init__(**kwargs)
        self.blob_data_loader = blob_data_loader

    async def batch_load_fn(self, keys):
        raw_datas = await self.blob_data_loader.load_many(keys)
        # We expect each "blob" here to be well-formed JSON, so parse each of them thusly
        # TODO - streaming?
        return [json.loads(next(raw_data)) for raw_data in raw_datas]


RawJSONLinesDataLoader = TypeVar("LinesDataLoader", LocalFileLinesDataLoader, S3FileLinesDataLoader)


class JSONLinesDataLoader(DataLoader, Generic[RawJSONLinesDataLoader]):
    lines_data_loader: RawJSONLinesDataLoader

    def __init__(self, lines_data_loader: RawJSONLinesDataLoader, **kwargs):
        super().__init__(**kwargs)
        self.lines_data_loader = lines_data_loader

    async def batch_load_fn(self, keys):
        raw_datas = await self.lines_data_loader.load_many(keys)
        # TODO - comment
        all_data = []
        for data in raw_datas:
            parsed_lines = []
            # print(data)
            # TODO - make this streaming/a generator?
            for line in data:
                # print(line)
                parsed_lines.append(json.loads(line))

            all_data.append(parsed_lines)

        return all_data


class JSONStreamingLinesDataLoader(DataLoader):

    _http_data_loader: HTTPStreamingDataLoader

    def __init__(self, http_data_loader: HTTPStreamingDataLoader):
        self._http_data_loader = http_data_loader or HTTPStreamingDataLoader()

    async def batch_load_fn(self, keys):
        streams = await self.http_data_loader.load_many(keys)
        # TODO - comment
        # all_data = []
        # for data in raw_datas:
        #     parsed_lines = []
        #     for line in data:
        #         parsed_lines.append(json.loads(line))
        #
        #     all_data.append(parsed_lines)
        #
        # return all_data
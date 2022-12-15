import logging
import orjson
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

    def yield_json(self, data):
        _, blob = data
        return orjson.loads(next(blob))


    async def batch_load_fn(self, keys):
        raw_datas = await self.blob_data_loader.load_many(keys)
        # We expect each "blob" here to be well-formed JSON, so parse each of them thusly
        return [self.yield_json(next(raw_data)) for raw_data in raw_datas]


RawJSONLinesDataLoader = TypeVar("LinesDataLoader", LocalFileLinesDataLoader, S3FileLinesDataLoader,
                                 HTTPFileLinesDataLoader)


class JSONLinesDataLoader(DataLoader, Generic[RawJSONLinesDataLoader]):
    cache = False
    lines_data_loader: RawJSONLinesDataLoader

    def yield_json_lines(self, filename, data):
        for filepath, lines in data:
            self.logger.info(f"Processing: {filepath}")

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
                    self.logger.warning(f"Could not parse file {filepath} as JSON - skipping")
                finally:
                    continue

            for line in lines:
                yield orjson.loads(line)

            # for line in lines:
            #     try:
            #         yield orjson.loads(line)
            #     # Note - this is largely here because we may get arbitrary file types in archives where we are
            #     #  expecting eventlog files (which are JSON Lines). If we try to load those "lines" as
            #     #  JSON objects, they will fail, and so this allows us to just skip those "bad" lines and
            #     #  continue processing other files in the archive. However, this `except` could maybe be
            #     #  more robust so that we aren't ignoring real issues! If we do encounter some bad lines,
            #     #  we will at the very least log how many we hit
            #     except orjson.JSONDecodeError:
            #         num_bad_lines_seen += 1
            #         # We have to continue here because these will be the lines for the top-level file. If this is an
            #         #  archive, then it is possible for some lines to not be parse-able as JSON, but for others to be
            #         continue

        # if num_bad_lines_seen:
        #     self.logger.info(f"Failed to parse {num_bad_lines_seen} JSON lines in top-level file: {filename}")

    def __init__(self, lines_data_loader: RawJSONLinesDataLoader, **kwargs):
        super().__init__(**kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.lines_data_loader = lines_data_loader

    async def batch_load_fn(self, keys):
        raw_datas = await self.lines_data_loader.load_many(keys)
        return [self.yield_json_lines(key, data) for (key, data) in zip(keys, raw_datas)]

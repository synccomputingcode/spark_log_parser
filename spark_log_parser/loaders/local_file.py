import zipfile
from abc import ABC

from pathlib import Path
from urllib.parse import ParseResult, urlparse

from memory_profiler import profile

from spark_log_parser.loaders import AbstractFileDataLoader, AbstractLinesDataLoader, AbstractBlobDataLoader


class AbstractLocalFileDataLoader(AbstractFileDataLoader, ABC):
    """

    """
    cache = False

    def load_item(self, filepath: str | ParseResult):
        parsed_url: ParseResult = filepath if isinstance(filepath, ParseResult) else urlparse(filepath)
        path: Path = Path(parsed_url.path)

        yield from self.extract(path)


class LocalFileBlobDataLoader(AbstractLocalFileDataLoader, AbstractBlobDataLoader):
    pass


class LocalFileLinesDataLoader(AbstractLocalFileDataLoader, AbstractLinesDataLoader):
    pass

from abc import ABC
from pathlib import Path
from urllib.parse import ParseResult, urlparse


from spark_log_parser.loaders import AbstractFileDataLoader, AbstractLinesDataLoader, AbstractBlobDataLoader


class AbstractLocalFileDataLoader(AbstractFileDataLoader, ABC):
    """
    Abstract class for loading files that are on a local disk. This assumes that all we need
    is a path to the top-level file/directory from which we can extract data.
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

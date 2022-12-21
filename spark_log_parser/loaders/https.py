import abc
from pathlib import Path

import requests
from urllib.parse import ParseResult, urlparse

from spark_log_parser.loaders import AbstractFileDataLoader, BlobFileReaderMixin, LinesFileReaderMixin


class AbstractHTTPFileDataLoader(AbstractFileDataLoader, abc.ABC):
    """
    Abstract class that implements the `load_item` method for fetching files over HTTP/S
    """

    def _validate_url(self, url: ParseResult | str) -> ParseResult:
        parsed_url: ParseResult = url if isinstance(url, ParseResult) else urlparse(url)
        if parsed_url.scheme not in {"http", "https"}:
            raise ValueError(f"URL scheme '{parsed_url.scheme}' is not one of {', '.join(self.ALLOWED_SCHEMES)}")

    def load_item(self, url):
        self._validate_url(url)
        response = requests.get(url, stream=True)
        response.raise_for_status()

        if not int(response.headers.get("Content-Length", 0)):
            raise AssertionError("Download is empty")

        stream = response.raw
        yield from self.extract(Path(url), stream)


class HTTPFileBlobDataLoader(AbstractHTTPFileDataLoader, BlobFileReaderMixin):
    """
    Simple HTTP loader that returns the full file as a blob of data.
    """


class HTTPFileLinesDataLoader(AbstractHTTPFileDataLoader, LinesFileReaderMixin):
    """
    Simple HTTP loader that returns the file as a stream of lines (delimited by `\n`).
    """

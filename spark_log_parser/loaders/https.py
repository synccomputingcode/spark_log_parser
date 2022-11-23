import abc
import requests
from urllib.parse import ParseResult, urlparse

from aiodataloader import DataLoader


class AbstractHTTPFileDataLoader(abc.ABC, DataLoader):
    """

    """

    _http_chunk_size: int = 1024 * 1024
    _stream_responses: bool = True

    @abc.abstractmethod
    def transform_response_content(self, content):
        """
        Abstract method that allows for interpreting the response content in some way before returning the content itself
        """
        return content

    @abc.abstractmethod
    def iterate_response(self, response):
        """

        """
        return response.iter_content(chunk_size=self._http_chunk_size)

    def _validate_url(self, url: ParseResult | str) -> ParseResult:
        parsed_url: ParseResult = url if isinstance(url, ParseResult) else urlparse(url)
        if parsed_url.scheme not in {"http", "https"}:
            raise ValueError(
                "URL scheme '%s' is not one of {'%s'}"
                % (parsed_url.scheme, "', '".join(self.ALLOWED_SCHEMES))
            )

        return parsed_url

    def load_over_http(self, url):
        url = self._validate_url(url)
        response = requests.get(url.geturl(), stream=self._stream_responses)
        response.raise_for_status()

        if not int(response.headers.get("Content-Length", 0)):
            raise AssertionError("Download is empty")

        content = self.iterate_response(response)
        return self.transform_response_content(content)

    async def batch_load_fn(self, urls) -> list:
        return [self.load_over_http(url) for url in urls]


class HTTPStreamingDataLoader(AbstractHTTPFileDataLoader):
    """

    """

    def transform_response_content(self, response):
        return super().transform_response_content(response)

    def iterate_response(self, response):
        return super().iterate_response(response)


class HTTPBlobDataLoader(AbstractHTTPFileDataLoader):
    """
    Simple HTTP loader that returns the full response as a blob of data. This Loader does not stream responses,
    so we just take the first chunk of the response as the blob
    """
    _stream_responses = False

    def transform_response_content(self, response):
        return response[0]

    def iterate_response(self, response):
        return super().iterate_response(response)


class HTTPLinesDataLoader(AbstractHTTPFileDataLoader):

    def transform_response_content(self, response):
        return super().transform_response_content(response)

    def iterate_response(self, response):
        return response.iter_lines()

from pathlib import Path

import pytest

from spark_log_parser.loaders import FileExtractionResult
from spark_log_parser.loaders.local_file import LocalFileLinesDataLoader
from tests import ROOT_DIR
from tests.conftest import zip_to_tgz


@pytest.fixture(scope="module", autouse=True)
def archive_paths():
    zip_archive_path = Path(ROOT_DIR, "logs", "databricks-rollover-messy.zip")
    tarball = zip_to_tgz(zip_archive_path, file_suffix=".tgz")
    tarball_path = Path(tarball.name)

    yield [zip_archive_path, tarball_path]

    # Make sure to remove this temp file after the test in this module are done
    tarball_path.unlink(missing_ok=True)


@pytest.mark.asyncio
async def test_file_streams_fully_consumed(archive_paths):
    """
    Test to ensure that consumers of loaded files do not need to manually consume any `FileStreamIterator`s
    manually
    """
    file_loader = LocalFileLinesDataLoader()
    data: list[FileExtractionResult] = await file_loader.load_many([str(path) for path in archive_paths])
    for result in data:
        for (fname, fs_iter) in result:
            assert fname

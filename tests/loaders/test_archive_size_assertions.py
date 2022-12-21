from pathlib import Path
from typing import Iterator

import pytest

from spark_log_parser.loaders import ArchiveExtractionThresholds, ArchiveTooLargeError, ArchiveTooManyEntriesError
from spark_log_parser.loaders.local_file import LocalFileBlobDataLoader
from tests import ROOT_DIR
from tests.conftest import zip_to_tgz


@pytest.fixture(scope="module", autouse=True)
def archive_paths():
    zip_archive_path = Path(ROOT_DIR, "logs", "databricks.zip")
    tarball = zip_to_tgz(zip_archive_path, file_suffix=".tgz")
    tarball_path = Path(tarball.name)

    yield [zip_archive_path, tarball_path]

    # Make sure to remove this temp file after the test in this module are done
    tarball_path.unlink(missing_ok=True)


@pytest.mark.asyncio
async def test_loading_succeeds_as_expected(archive_paths):
    archive_thresholds = ArchiveExtractionThresholds()
    file_loader = LocalFileBlobDataLoader(extraction_thresholds=archive_thresholds)
    for path in archive_paths:
        file_iter = await file_loader.load(str(path))
        assert isinstance(file_iter, Iterator), \
            f"Expected to receive an iterator of the file's bytes, but received instead: {file_iter}"

        # Need to actually consume the iterator in order to test this behaviour; otherwise, the file is not
        #  actually ever read in from disk
        for _ in file_iter:
            continue


@pytest.mark.asyncio
async def test_archive_size_limit_error(archive_paths):
    archive_thresholds = ArchiveExtractionThresholds(size=1)
    file_loader = LocalFileBlobDataLoader(extraction_thresholds=archive_thresholds)
    for path in archive_paths:
        try:
            for _ in await file_loader.load(str(path)):
                continue
            assert False, f"Expected an ArchiveTooLargeError error to be raised while loading filepath: {path}"
        except ArchiveTooLargeError as e:
            assert e


@pytest.mark.asyncio
async def test_archive_members_limit(archive_paths):
    archive_thresholds = ArchiveExtractionThresholds(entries=0)
    file_loader = LocalFileBlobDataLoader(extraction_thresholds=archive_thresholds)
    for path in archive_paths:
        try:
            for _ in await file_loader.load(str(path)):
                continue
            assert False, f"Expected an ArchiveTooManyEntriesError to be raised while loading filepath: {path}"
        except ArchiveTooManyEntriesError as e:
            assert e

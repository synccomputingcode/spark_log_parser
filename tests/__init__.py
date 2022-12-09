import tempfile
import zipfile
import tarfile
import os.path
import logging
import pytest
from pathlib import Path
from pprint import pformat
from deepdiff import DeepDiff

from spark_log_parser.parsing_models.application_model_v2 import SparkApplication

logging.basicConfig()
logging.root.setLevel(logging.INFO)

# All the top-level keys that we would expect to be present in the JSON representation of a parsed SparkApplication
PARSED_KEYS = [
    "accumData",
    "executors",
    "jobData",
    "metadata",
    "sqlData",
    "stageData",
    "taskData",
]

# In parsed log files (i.e. SparkApplication.to_dict()), this is the set of top-level keys for which we would expect
# members to not necessarily always be ordered the same way
UNORDERED_PARSED_KEYS = [
    "accumData",
    "sqlData",
    "executors"
]

APP_NAME_INCORRECT_MESSAGE = "Application name does not match expected value"
PARSED_KEYS_MISSING_MESSAGE = "Not all keys present in parsed eventlog"


def assert_all_files_identical(parsed_files):
    """
    Given a list of already-parsed eventlog file dictionaries (i.e. SparkApplication.to_dict()), assert that all
    dictionaries are meaningfully the same, ignoring ordering of keys as is applicable
    """
    [first, *rest] = parsed_files
    for curr in rest:
        for key, curr_value in curr.items():
            first_value = first[key]

            # This is much faster than a DeepDiff, and since we expect these values to match most of the time, this
            #  should help speed up tests in the common-case that things are working as expected
            if curr_value == first_value:
                continue

            diff = DeepDiff(curr_value, first_value)
            # If we get a diff, we should double-check that that diff isn't just due to ordering differences, which may
            #  not be an issue at all. This should help more readily pinpoint what the actual issue in any detected diff
            #  may be
            if diff:
                diff_ignoring_order = DeepDiff(curr_value, first_value, ignore_order=True)

            if diff and not diff_ignoring_order and key not in UNORDERED_PARSED_KEYS:
                raise ValueError(f"Detected an ordering difference for key: {key} in parsed log files.\n" +
                                 f"If this is expected, please update UNORDERED_PARSED_KEYS. Otherwise, please " +
                                 "fix this diff.\n"
                                 f"{pformat(diff)}")

            if diff and diff_ignoring_order:
                raise ValueError(f"Detected a difference not due to ordering for key: {key} in parsed log files\n" +
                                 f"{pformat(diff_ignoring_order)}")


def zip_to_tgz(zip_path: str | Path, file_suffix: str):
    """
    Given a zip archive residing at the provided Path, converts to a .tgz archive
    """
    zip_path = zip_path if isinstance(zip_path, Path) else Path(zip_path)
    with tempfile.TemporaryDirectory() as temp_dir:
        with zipfile.ZipFile(zip_path) as zfile:
            zfile.extractall(temp_dir)
            # Grab the last part of the original path to use as the filename for the new one
            tgz_filename = zip_path.parts[-1]
            # Remove suffixes from the original file name in reverse order
            for suffix in zip_path.suffixes[::-1]:
                tgz_filename = tgz_filename.removesuffix(suffix)

            tgz_filename += file_suffix
            with tarfile.open(tgz_filename, "w:gz") as tarball:
                tarball.add(temp_dir, arcname=os.path.sep)
                return tarball

@pytest.fixture
def parsed_files(request) -> list[dict | SparkApplication]:
    """
    Given a reference to some .zip file and some parsing function, this fixture will -
    - Parse the .zip archive using the given `parse_fn`,
    - Convert the .zip archive to other archive formats we support (right now - .tgz / .tar.gz) and parse those,
        - Remove any files written to disk,
    - Return a list of all the parsed files
    """
    (zip_path, parse_fn) = request.param
    parsed_zip = parse_fn(zip_path)

    tgz_archives = []
    for ext in [".tgz", ".tar.gz"]:
        try:
            tgz = zip_to_tgz(zip_path, ext)
            filepath = Path(tgz.name)
            parsed = parse_fn(filepath)
            tgz_archives.append(parsed)
        finally:
            # We have the parsed, in-memory representation - that means we can remove the file we generated on disk
            #  and return only the parsed app to the test. We put this in a `finally` block so that the temp file
            #  on disk is always removed
            filepath.unlink(missing_ok=True)

    return [parsed_zip, *tgz_archives]

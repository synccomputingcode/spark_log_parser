import tempfile
import zipfile
import tarfile
import os.path
from pathlib import Path
from pprint import pprint, pformat

import pytest

from deepdiff import DeepDiff

from spark_log_parser.parsing_models.application_model_v2 import create_spark_application, SparkApplication

PARSED_KEYS = [
    "accumData",
    "executors",
    "jobData",
    "metadata",
    "sqlData",
    "stageData",
    "taskData",
]

UNORDERED_PARSED_KEYS = [
    # "accumData"
]


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


def get_spark_app_from_raw_log(event_log_path):
    return create_spark_application(spark_eventlog_path=str(event_log_path))


def get_parsed_log(event_log_path):
    return get_spark_app_from_raw_log(event_log_path).to_dict()


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
            # and return only the parsed app to the test
            filepath.unlink(missing_ok=True)

    return [parsed_zip, *tgz_archives]


@pytest.mark.parametrize("parsed_files",
                         [(Path("tests", "logs", "databricks.zip").resolve(), get_parsed_log)],
                         indirect=["parsed_files"])
def test_simple_databricks_log(parsed_files):
    for file in parsed_files:
        assert all(key in file for key in PARSED_KEYS), "Not all keys are present"
        assert (
            file["metadata"]["application_info"]["name"] == "Databricks Shell"
        ), "Name is as expected"

    assert all(file == parsed_files[0] for file in
               parsed_files[1:]), "Expected all parsed files to be the same, but they were not"


@pytest.mark.parametrize("parsed_files",
                         [(Path("tests", "logs", "emr.zip").resolve(), get_parsed_log)],
                         indirect=["parsed_files"])
def test_simple_emr_log(parsed_files):
    for file in parsed_files:
        assert all(key in file for key in PARSED_KEYS), "Not all keys are present"
        assert (
            file["metadata"]["application_info"]["name"] == "Text Similarity"
        ), "Name is as expected"

    assert all(file == parsed_files[0] for file in
               parsed_files[1:]), "Expected all parsed files to be the same, but they were not"


@pytest.mark.parametrize("parsed_files",
                         [(Path("tests", "logs", "emr_missing_sql_events.zip").resolve(), get_spark_app_from_raw_log)],
                         indirect=["parsed_files"])
def test_emr_missing_sql_events(parsed_files):
    for spark_app in parsed_files:
        sql_data = spark_app.sqlData
        assert sql_data.index.name == "sql_id"
        assert list(sql_data.index.values) == [0, 2, 3, 5, 6, 7, 8]

@pytest.mark.parametrize("parsed_files",
                         [(Path("tests", "logs", "databricks-rollover-messy.zip").resolve(), get_parsed_log)],
                         indirect=["parsed_files"])
def test_databricks_rollover(parsed_files):
    # for file in parsed_files:
    #     assert all(key in file for key in PARSED_KEYS), "Not all keys are present"
    #     assert (
    #         file["metadata"]["application_info"]["name"] == "Text Similarity"
    #     ), "Name is as expected"

    [first, *rest] = parsed_files
    for curr in rest:
        for key, curr_value in curr.items():
            first_value = first[key]

            if curr_value == first_value:
                continue

            diff = DeepDiff(curr_value, first_value)
            if diff:
                diff_ignoring_order = DeepDiff(curr_value, first_value, ignore_order=True)

            if diff and not diff_ignoring_order and key not in UNORDERED_PARSED_KEYS:
                raise ValueError(f"Detected an ordering difference for key: {key} in parsed log files\n" +
                                 f"If this is expected, please update UNORDERED_PARSED_KEYS. Otherwise, please " +
                                 "fix this diff\n"
                                 f"{pformat(diff)}")

            if diff and diff_ignoring_order:
                raise ValueError(f"Detected a difference not due to ordering for key: {key} in parsed log files\n" +
                                 f"{pformat(diff)}")


            # diff = DeepDiff(curr_value, first_value, ignore_order=True)
            # if key == "accumData":
            # else:
            #     diff = DeepDiff(curr_value, first_value)
            # pprint(diff)

            # assert not diff, f"Expected no diff for values of serialized SparkApplications, but key: {key} had diffs"

        # diff = DeepDiff(first_file, file)
        # pprint(diff)
        # assert curr == first, f"Expected all parsed files to be the same, but they were not."


def test_parsed_log():
    """
    Test that re-hydrating a parsed spark application contains all the keys we would expect it to
    """
    event_log_path = Path("tests", "logs", "similarity_parsed.json.gz").resolve()
    rehydrated = create_spark_application(spark_eventlog_parsed_path=str(event_log_path)).to_dict()
    assert all(key in rehydrated for key in PARSED_KEYS), "Not all keys are present in re-hydrated Spark application"

import asyncio
import requests_mock
from pathlib import Path

import pytest

from spark_log_parser.loaders.https import HTTPFileLinesDataLoader, HTTPFileBlobDataLoader
from spark_log_parser.loaders.json import JSONLinesDataLoader, JSONBlobDataLoader
from spark_log_parser.parsing_models.application_model_v2 import \
    UnparsedLogSparkApplicationLoader, SparkApplication, \
    ParsedLogSparkApplicationLoader, AmbiguousLogFormatSparkApplicationLoader
from tests import parsed_files, assert_all_files_identical, PARSED_KEYS


def get_spark_app(eventlog_local_path) -> SparkApplication:
    file_size = eventlog_local_path.stat().st_size
    http_url = f"https://sync-test-artifacts.s3.amazonaws.com/{str(eventlog_local_path)}"

    with open(eventlog_local_path, "rb") as fobj:
        file_content = fobj.read()

        with requests_mock.Mocker() as mock_request:
            mock_request.get(http_url, headers={"Content-Length": str(file_size)},
                             content=lambda request, content: file_content)

            async def create_spark_app():
                lines_loader = HTTPFileLinesDataLoader()
                json_loader = JSONLinesDataLoader(lines_data_loader=lines_loader)
                app_loader = AmbiguousLogFormatSparkApplicationLoader(json_lines_loader=json_loader)
                return await app_loader.load(http_url)

            spark_app = asyncio.run(create_spark_app())
            return spark_app


def get_parsed_log(eventlog_local_path) -> dict:
    spark_app = get_spark_app(eventlog_local_path)
    return spark_app.to_dict()


@pytest.mark.parametrize("parsed_files",
                         [(Path("tests", "logs", "emr.zip").resolve(), get_parsed_log)],
                         indirect=["parsed_files"])
def test_simple_emr_log(parsed_files):
    assert_all_files_identical(parsed_files)


@pytest.mark.parametrize("parsed_files",
                         [(Path("tests", "logs", "emr_missing_sql_events.zip").resolve(), get_spark_app)],
                         indirect=["parsed_files"])
def test_emr_missing_sql_events(parsed_files):
    for spark_app in parsed_files:
        sql_data = spark_app.sqlData
        assert sql_data.index.name == "sql_id"
        assert list(sql_data.index.values) == [0, 2, 3, 5, 6, 7, 8]

    parsed_apps = [spark_app.to_dict() for spark_app in parsed_files]
    assert_all_files_identical(parsed_apps)


@pytest.mark.parametrize("parsed_files",
                         [(Path("tests", "logs", "databricks.zip").resolve(), get_parsed_log)],
                         indirect=["parsed_files"])
def test_simple_databricks_log(parsed_files):
    assert_all_files_identical(parsed_files)


@pytest.mark.parametrize("parsed_files",
                         [(Path("tests", "logs", "databricks-rollover-messy.zip").resolve(), get_parsed_log)],
                         indirect=["parsed_files"])
def test_databricks_rollover_log(parsed_files):
    assert_all_files_identical(parsed_files)

    # For this rollover log test case, we know that there are 71 jobs spread across the rollover logs. Therefore, in
    #  order to assert that we read all of them, we can just check that job_ids 0 - 71 exist in the SparkApplication's
    #  jobData. If some are missing, that likely means we missed at least one of the files
    for logfile in parsed_files:
        expected_to_see = set(range(72))
        actual_job_ids = set(logfile["jobData"]["job_id"])
        missing_ids = expected_to_see.difference(actual_job_ids)
        assert not missing_ids, f"Expected all job_ids to be present in parsed jobData, but these IDs were missing: {missing_ids}"


def test_parsed_log():
    """
    Test that re-hydrating a parsed spark application contains all the keys we would expect it to
    """
    eventlog_local_path = Path("tests", "logs", "similarity_parsed.json.gz").resolve()
    file_size = eventlog_local_path.stat().st_size
    http_url = f"https://sync-test-artifacts.s3.amazonaws.com/{str(eventlog_local_path)}"

    with open(eventlog_local_path, "rb") as fobj:
        file_content = fobj.read()

        with requests_mock.Mocker() as mock_request:
            mock_request.get(http_url, headers={"Content-Length": str(file_size)},
                             content=lambda request, content: file_content)

            async def create_spark_app():
                blob_loader = HTTPFileBlobDataLoader()
                json_loader = JSONBlobDataLoader(blob_data_loader=blob_loader)
                app_loader = ParsedLogSparkApplicationLoader(json_loader=json_loader)
                return await app_loader.load(http_url)

            spark_app = asyncio.run(create_spark_app())
            parsed_app = spark_app.to_dict()

    assert all(key in parsed_app for key in PARSED_KEYS), "Not all keys are present in re-hydrated Spark application"


def test_ambiguous_log_format_parsed_log():
    """
    In real-world scenarios, we may not know if an eventlog is parsed or unparsed without trying to process the file
    first. This test exercises that disambiguation logic when given a parsed eventlog
    """
    eventlog_local_path = Path("tests", "logs", "similarity_parsed.json.gz").resolve()
    parsed_app = get_parsed_log(eventlog_local_path)
    assert all(key in parsed_app for key in PARSED_KEYS), "Not all keys are present in re-hydrated Spark application"

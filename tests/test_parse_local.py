from pathlib import Path

import pytest

from spark_log_parser.parsing_models.application_model_v2 import create_spark_application
from tests import assert_all_files_equivalent, PARSED_KEYS, APP_NAME_INCORRECT_MESSAGE, PARSED_KEYS_MISSING_MESSAGE, \
    ROOT_DIR


def get_spark_app_from_raw_log(event_log_path):
    return create_spark_application(spark_eventlog_path=str(event_log_path))


def get_parsed_log(event_log_path):
    return get_spark_app_from_raw_log(event_log_path).to_dict()


def test_simple_databricks_log():
    path = Path(ROOT_DIR, "logs", "databricks.json").resolve()
    parsed_app = get_parsed_log(path)
    assert all(key in parsed_app for key in PARSED_KEYS), PARSED_KEYS_MISSING_MESSAGE
    assert (
        parsed_app["metadata"]["application_info"]["name"] == "Databricks Shell"
    ), APP_NAME_INCORRECT_MESSAGE


@pytest.mark.parametrize("parsed_files",
                         [(Path(ROOT_DIR, "logs", "databricks.zip").resolve(), get_parsed_log)],
                         indirect=["parsed_files"])
def test_simple_databricks_archive(parsed_files):
    for file in parsed_files:
        assert all(key in file for key in PARSED_KEYS), PARSED_KEYS_MISSING_MESSAGE
        assert (
            file["metadata"]["application_info"]["name"] == "Databricks Shell"
        ), APP_NAME_INCORRECT_MESSAGE

    assert all(file == parsed_files[0] for file in
               parsed_files[1:]), "Expected all parsed files to be the same, but they were not"


@pytest.mark.parametrize("parsed_files",
                         [(Path(ROOT_DIR, "logs", "emr.zip").resolve(), get_parsed_log)],
                         indirect=["parsed_files"])
def test_simple_emr_log(parsed_files):
    for file in parsed_files:
        assert all(key in file for key in PARSED_KEYS), PARSED_KEYS_MISSING_MESSAGE
        assert (
            file["metadata"]["application_info"]["name"] == "Text Similarity"
        ), APP_NAME_INCORRECT_MESSAGE

    assert all(file == parsed_files[0] for file in
               parsed_files[1:]), "Expected all parsed files to be the same, but they were not"


@pytest.mark.parametrize("parsed_files",
                         [(Path(ROOT_DIR, "logs", "emr_missing_sql_events.zip").resolve(), get_spark_app_from_raw_log)],
                         indirect=["parsed_files"])
def test_emr_missing_sql_events(parsed_files):
    assert_all_files_equivalent([spark_app.to_dict() for spark_app in parsed_files])

    for spark_app in parsed_files:
        sql_data = spark_app.sqlData
        assert sql_data.index.name == "sql_id"
        assert list(sql_data.index.values) == [0, 2, 3, 5, 6, 7, 8]


@pytest.mark.parametrize("parsed_files",
                         [(Path(ROOT_DIR, "logs", "databricks-rollover-messy.zip").resolve(), get_parsed_log)],
                         indirect=["parsed_files"])
def test_databricks_rollover(parsed_files):
    assert_all_files_equivalent(parsed_files)


def test_parsed_log():
    """
    Test that re-hydrating a parsed spark application contains all the keys we would expect it to
    """
    event_log_path = Path(ROOT_DIR, "logs", "similarity_parsed.json.gz").resolve()
    rehydrated = create_spark_application(spark_eventlog_parsed_path=str(event_log_path)).to_dict()
    assert all(key in rehydrated for key in PARSED_KEYS), "Not all keys are present in re-hydrated Spark application"

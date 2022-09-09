import json
import tempfile
from pathlib import Path

from spark_log_parser import eventlog, extractor
from spark_log_parser.parsing_models.application_model_v2 import sparkApplication


def get_parsed_log(event_log_path):

    with tempfile.TemporaryDirectory() as temp_dir:
        event_log_paths = extractor.Extractor(event_log_path.resolve().as_uri(), temp_dir).extract()
        event_log = eventlog.EventLogBuilder(event_log_paths, temp_dir).build()

        result_path = str(Path(temp_dir, "result"))
        sparkApplication(eventlog=str(event_log)).save(result_path)

        with open(result_path + ".json") as result_fobj:
            parsed = json.load(result_fobj)

    return parsed


def test_simple_databricks_log():
    event_log_path = Path("tests", "logs", "databricks.zip").resolve()

    parsed = get_parsed_log(event_log_path)

    assert all(
        key in parsed
        for key in [
            "accumData",
            "executors",
            "jobData",
            "metadata",
            "sqlData",
            "stageData",
            "taskData",
        ]
    ), "Not all keys are present"

    assert (
        parsed["metadata"]["application_info"]["name"] == "Databricks Shell"
    ), "Name is as expected"


def test_simple_emr_log():
    event_log_path = Path("tests", "logs", "emr.zip").resolve()

    parsed = get_parsed_log(event_log_path)

    assert all(
        key in parsed
        for key in [
            "accumData",
            "executors",
            "jobData",
            "metadata",
            "sqlData",
            "stageData",
            "taskData",
        ]
    ), "Not all keys are present"

    assert (
        parsed["metadata"]["application_info"]["name"] == "Text Similarity"
    ), "Name is as expected"


def test_emr_missing_sql_events():
    event_log_path = Path("tests", "logs", "emr_missing_sql_events.zip").resolve()

    with tempfile.TemporaryDirectory() as temp_dir:
        event_log_paths = extractor.Extractor(event_log_path.resolve().as_uri(), temp_dir).extract()
        event_log = eventlog.EventLogBuilder(event_log_paths, temp_dir).build()
        obj = sparkApplication(eventlog=str(event_log))

    assert list(obj.sqlData.index.values) == [0, 2, 3, 5, 6, 7, 8]

import json
from pathlib import Path
from spark_log_parser import eventlog
import tempfile

from spark_log_parser.parsing_models.application_model_v2 import sparkApplication


def test_simple_databricks_log():
    event_log = Path("tests", "logs", "databricks.zip").resolve()

    with tempfile.TemporaryDirectory(prefix="eventlog-%s-" % event_log.name[:-len("".join(event_log.suffixes))]) as temp_dir:
        log = eventlog.EventLog(source_url=event_log.as_uri(), work_dir=temp_dir)

        result_path = str(Path(temp_dir, "result"))
        sparkApplication(eventlog=str(log.event_log)).save(result_path)

        with open(result_path + ".json") as result_fobj:
            parsed = json.load(result_fobj)

    assert all(key in parsed for key in ["accumData", "executors", "jobData", "metadata", "sqlData", "stageData", "taskData"]), "All keys are present"

    assert parsed['metadata']['application_info']['name'] == "Databricks Shell", "Name is as expected"

def test_simple_emr_log():
    event_log = Path("tests", "logs", "emr.zip").resolve()

    with tempfile.TemporaryDirectory(prefix="eventlog-%s-" % event_log.name[:-len("".join(event_log.suffixes))]) as temp_dir:
        log = eventlog.EventLog(source_url=event_log.as_uri(), work_dir=temp_dir)

        result_path = str(Path(temp_dir, "result"))
        sparkApplication(eventlog=str(log.event_log)).save(result_path)

        with open(str(result_path) + ".json") as result_fobj:
            parsed = json.load(result_fobj)

    assert all(key in parsed for key in ["accumData", "executors", "jobData", "metadata", "sqlData", "stageData", "taskData"]), "All keys are present"

    assert parsed['metadata']['application_info']['name'] == "Text Similarity", "Name is as expected"

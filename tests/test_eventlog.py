import json
from pathlib import Path
from spark_log_parser import eventlog
import tempfile


def test_simple_databricks_log():
    event_log = Path("tests", "logs", "databricks.zip").resolve()

    with tempfile.TemporaryDirectory(prefix="eventlog-%s-" % event_log.name[:-len("".join(event_log.suffixes))]) as temp_dir:
        log = eventlog.EventLog(url=event_log.as_uri(), work_dir=temp_dir)

        with open(log.event_log) as log_fobj:
            event = json.loads(log_fobj.readline())

    assert log.file_total == 1, "One file in log archive"

    assert all(key in event for key in ["Event", "Spark Version", "Timestamp", "Rollover Number", "SparkContext Id"]), "All keys are present"

    assert event['Event'] == "DBCEventLoggingListenerMetadata", "Expected first event is present"

def test_simple_emr_log():
    event_log = Path("tests", "logs", "emr.zip").resolve()

    with tempfile.TemporaryDirectory(prefix="eventlog-%s-" % event_log.name[:-len("".join(event_log.suffixes))]) as temp_dir:
        log = eventlog.EventLog(url=event_log.as_uri(), work_dir=temp_dir)

        with open(log.event_log) as log_fobj:
            event = json.loads(log_fobj.readline())

    assert log.file_total == 1, "One file in log archive"

    assert all(key in event for key in ["Event", "Spark Version"]), "All keys are present"

    assert event['Event'] == "SparkListenerLogStart", "Expected first event is present"

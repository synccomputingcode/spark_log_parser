import json
from pathlib import Path
from spark_log_parser import eventlog
import tempfile


def test_simple_databricks_log():
    event_log = Path("tests", "logs", "databricks.zip").resolve()

    with tempfile.TemporaryDirectory(prefix="eventlog-%s-" % event_log.name[:-len("".join(event_log.suffixes))]) as temp_dir:
        log = eventlog.EventLog(source_url=event_log.as_uri(), work_dir=temp_dir)

        with open(log.event_log) as log_fobj:
            event = json.loads(log_fobj.readline())

    assert log.file_total == 1, "One file in log archive"

    assert all(key in event for key in ["Event", "Spark Version", "Timestamp", "Rollover Number", "SparkContext Id"]), "All keys are present"

    assert event['Event'] == "DBCEventLoggingListenerMetadata", "Expected first event is present"


def test_simple_emr_log():
    event_log = Path("tests", "logs", "emr.zip").resolve()

    with tempfile.TemporaryDirectory(prefix="eventlog-%s-" % event_log.name[:-len("".join(event_log.suffixes))]) as temp_dir:
        log = eventlog.EventLog(source_url=event_log.as_uri(), work_dir=temp_dir)

        with open(log.event_log) as log_fobj:
            event = json.loads(log_fobj.readline())

    assert log.file_total == 1, "One file in log archive"

    assert all(key in event for key in ["Event", "Spark Version"]), "All keys are present"

    assert event['Event'] == "SparkListenerLogStart", "Expected first event is present"


def test_databricks_rollover_log():
    event_log = Path("tests", "logs", "databricks-rollover.zip").resolve()

    with tempfile.TemporaryDirectory(prefix="eventlog-%s-" % event_log.name[:-len("".join(event_log.suffixes))]) as temp_dir:
        log = eventlog.EventLog(source_url=event_log.as_uri(), work_dir=temp_dir)

        with open(log.event_log) as log_fobj:
            event = json.loads(log_fobj.readline())
            log_fobj.seek(0)

            rollover_count = 0
            for i, event_str in enumerate(log_fobj):
                event = json.loads(event_str)
                if "Rollover Number" in event:
                    assert all(key in event for key in ["Event", "Spark Version", "Timestamp", "Rollover Number", "SparkContext Id"]), "All keys are present"
                    assert rollover_count == event['Rollover Number'], "Contiguous monotonically increasing IDs"
                    rollover_count += 1

            assert rollover_count == 3, "All log parts are present"
            assert i + 1 == 16945, "All events are present"

    assert log.file_total == 4, "4 files in the log archive - 3 event log parts and 1 pricing file"


def test_databricks_messy_rollover_log_archive():
    event_log = Path("tests", "logs", "databricks-rollover-messy.zip").resolve()

    with tempfile.TemporaryDirectory(prefix="eventlog-%s-" % event_log.name[:-len("".join(event_log.suffixes))]) as temp_dir:
        log = eventlog.EventLog(source_url=event_log.as_uri(), work_dir=temp_dir)

        with open(log.event_log) as log_fobj:
            event = json.loads(log_fobj.readline())
            log_fobj.seek(0)

            rollover_count = 0
            for i, event_str in enumerate(log_fobj):
                event = json.loads(event_str)
                if "Rollover Number" in event:
                    assert all(key in event for key in ["Event", "Spark Version", "Timestamp", "Rollover Number", "SparkContext Id"]), "All keys are present"
                    assert rollover_count == event['Rollover Number'], "Contiguous monotonically increasing IDs"
                    rollover_count += 1

            assert rollover_count == 3, "All log parts are present"
            assert i + 1 == 16945, "All events are present"

    assert log.file_total == 4, "4 files in the log archive - 3 event log parts and 1 pricing file"
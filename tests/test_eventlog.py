import json
import tempfile
from pathlib import Path

from spark_log_parser import eventlog


def test_simple_emr_log():
    event_log = Path("tests", "logs", "emr.zip").resolve()

    with tempfile.TemporaryDirectory(
        prefix="eventlog-%s-" % event_log.name[: -len("".join(event_log.suffixes))]
    ) as temp_dir:
        log = eventlog.EventLog(source_url=event_log.as_uri(), work_dir=temp_dir)

        with open(log.event_log) as log_fobj:
            event = json.loads(log_fobj.readline())

    assert not log.is_parsed, "Log is correctly identified as being not parsed"

    assert all(key in event for key in ["Event", "Spark Version"]), "All keys are present"

    assert event["Event"] == "SparkListenerLogStart", "Expected first event is present"


def test_simple_databricks_log():
    event_log = Path("tests", "logs", "databricks.zip").resolve()

    with tempfile.TemporaryDirectory(
        prefix="eventlog-%s-" % event_log.name[: -len("".join(event_log.suffixes))]
    ) as temp_dir:
        log = eventlog.EventLog(source_url=event_log.as_uri(), work_dir=temp_dir)

        with open(log.event_log) as log_fobj:
            event = json.loads(log_fobj.readline())

    assert not log.is_parsed, "Log is correctly identified as being not parsed"

    assert all(
        key in event
        for key in ["Event", "Spark Version", "Timestamp", "Rollover Number", "SparkContext Id"]
    ), "All keys are present"

    assert event["Event"] == "DBCEventLoggingListenerMetadata", "Expected first event is present"


def test_databricks_rollover_log():
    event_log = Path("tests", "logs", "databricks-rollover.zip").resolve()

    with tempfile.TemporaryDirectory(
        prefix="eventlog-%s-" % event_log.name[: -len("".join(event_log.suffixes))]
    ) as temp_dir:
        log = eventlog.EventLog(source_url=event_log.as_uri(), work_dir=temp_dir)

        with open(log.event_log) as log_fobj:
            event = json.loads(log_fobj.readline())
            log_fobj.seek(0)

            rollover_count = 0
            for i, event_str in enumerate(log_fobj):
                event = json.loads(event_str)
                if "Rollover Number" in event:
                    assert all(
                        key in event
                        for key in [
                            "Event",
                            "Spark Version",
                            "Timestamp",
                            "Rollover Number",
                            "SparkContext Id",
                        ]
                    ), "All keys are present"
                    assert (
                        rollover_count == event["Rollover Number"]
                    ), "Contiguous monotonically increasing IDs"
                    rollover_count += 1

            assert rollover_count == 3, "All log parts are present"
            assert i + 1 == 16945, "All events are present"


def test_databricks_messy_rollover_log_archive():
    event_log = Path("tests", "logs", "databricks-rollover-messy.zip").resolve()

    with tempfile.TemporaryDirectory(
        prefix="eventlog-%s-" % event_log.name[: -len("".join(event_log.suffixes))]
    ) as temp_dir:
        log = eventlog.EventLog(source_url=event_log.as_uri(), work_dir=temp_dir)

        with open(log.event_log) as log_fobj:
            event = json.loads(log_fobj.readline())
            log_fobj.seek(0)

            rollover_count = 0
            for i, event_str in enumerate(log_fobj):
                event = json.loads(event_str)
                if "Rollover Number" in event:
                    assert all(
                        key in event
                        for key in [
                            "Event",
                            "Spark Version",
                            "Timestamp",
                            "Rollover Number",
                            "SparkContext Id",
                        ]
                    ), "All keys are present"
                    assert (
                        rollover_count == event["Rollover Number"]
                    ), "Contiguous monotonically increasing IDs"
                    rollover_count += 1

            assert rollover_count == 3, "All log parts are present"
            assert i + 1 == 16945, "All events are present"


def test_raw_databricks_log():
    event_log = Path("tests", "logs", "databricks.json").resolve()

    with tempfile.TemporaryDirectory(
        prefix="eventlog-%s-" % event_log.name[: -len("".join(event_log.suffixes))]
    ) as temp_dir:
        log = eventlog.EventLog(source_url=event_log.as_uri(), work_dir=temp_dir)

        with open(log.event_log) as log_fobj:
            event = json.loads(log_fobj.readline())

    assert all(
        key in event
        for key in ["Event", "Spark Version", "Timestamp", "Rollover Number", "SparkContext Id"]
    ), "All keys are present"

    assert event["Event"] == "DBCEventLoggingListenerMetadata", "Expected first event is present"


def test_parsed_log():
    parsed_path = Path("tests", "logs", "databricks-parsed.json").resolve()

    with tempfile.TemporaryDirectory(
        prefix="eventlog-%s-" % parsed_path.name[: -len("".join(parsed_path.suffixes))]
    ) as temp_dir:
        log = eventlog.EventLog(source_url=parsed_path.as_uri(), work_dir=temp_dir)

        with open(log.event_log) as log_fobj:
            parsed = json.loads(log_fobj.readline())

    assert log.is_parsed, "Parsed flag is set"

    assert all(
        key in parsed for key in ["jobData", "stageData", "taskData"]
    ), "All keys are present"

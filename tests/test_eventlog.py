import json
import tempfile
import unittest
import zipfile
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

    assert all(
        key in event
        for key in ["Event", "Spark Version", "Timestamp", "Rollover Number", "SparkContext Id"]
    ), "All keys are present"


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
    assert event["Event"] == "DBCEventLoggingListenerMetadata", "Expected first event is present"


class RolloverLog(unittest.TestCase):
    def test_databricks_rollover_log(self):
        self.validate_log(Path("tests", "logs", "databricks-rollover.zip").resolve(), 3, 16945)

    def test_databricks_messy_rollover_log_archive(self):
        self.validate_log(
            Path("tests", "logs", "databricks-rollover-messy.zip").resolve(), 3, 16945
        )

    def test_databricks_messy_rollover_log_dir(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            with zipfile.ZipFile(Path("tests", "logs", "databricks-rollover-messy.zip")) as zinfo:
                temp_dir_path = Path(temp_dir)
                zinfo.extractall(temp_dir_path)
                self.validate_log(temp_dir_path, 3, 16945)

    def validate_log(self, event_log: Path, log_file_total: int, log_entry_total: int):
        with tempfile.TemporaryDirectory() as temp_dir:
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

                assert rollover_count == log_file_total, "All log parts are present"
                assert i + 1 == log_entry_total, "All events are present"

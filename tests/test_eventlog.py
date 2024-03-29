import json
import tempfile
import unittest
import zipfile
from pathlib import Path

from spark_log_parser import eventlog, extractor


def get_event(event_log_path):

    with tempfile.TemporaryDirectory() as temp_dir:
        event_log_paths = extractor.Extractor(event_log_path.resolve().as_uri(), temp_dir).extract()
        event_log, parsed = eventlog.EventLogBuilder(event_log_paths, temp_dir).build()

        with open(event_log) as log_fobj:
            event = json.loads(log_fobj.readline())

        return event, parsed


def test_simple_emr_log():
    event_log_path = Path("tests", "logs", "emr.zip").resolve()
    event, parsed = get_event(event_log_path)

    assert all(key in event for key in ["Event", "Spark Version"]), "Not all keys are present"
    assert event["Event"] == "SparkListenerLogStart", "First event is not as expected"
    assert not parsed


def test_simple_databricks_log():
    event_log_path = Path("tests", "logs", "databricks.zip").resolve()
    event, parsed = get_event(event_log_path)

    assert all(
        key in event
        for key in ["Event", "Spark Version", "Timestamp", "Rollover Number", "SparkContext Id"]
    ), "Not all keys are present"
    assert not parsed


def test_raw_databricks_log():
    event_log_path = Path("tests", "logs", "databricks.json").resolve()
    event, parsed = get_event(event_log_path)

    assert all(
        key in event
        for key in ["Event", "Spark Version", "Timestamp", "Rollover Number", "SparkContext Id"]
    ), "Not all keys are present"

    assert event["Event"] == "DBCEventLoggingListenerMetadata", "First event is not as expected"
    assert not parsed


def test_log_in_dir():
    event_log_path = Path("tests", "logs", "log_in_dir", "databricks.json.gz").resolve()
    event, parsed = get_event(event_log_path)

    assert all(
        key in event
        for key in ["Event", "Spark Version", "Timestamp", "Rollover Number", "SparkContext Id"]
    ), "Not all keys are present"

    assert event["Event"] == "DBCEventLoggingListenerMetadata", "First event is not as expected"
    assert not parsed


class RolloverLog(unittest.TestCase):
    def test_databricks_rollover_log(self):
        self.validate_log(Path("tests", "logs", "databricks-rollover.zip").resolve(), 3, 16945)

    def test_databricks_messy_rollover_log_archive(self):
        self.validate_log(
            Path("tests", "logs", "databricks-rollover-messy.zip").resolve(), 3, 16945
        )

    def test_databricks_messy_rollover_log_dir(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            with zipfile.ZipFile(Path("tests", "logs", "databricks-rollover-messy.zip")) as zfile:
                temp_dir_path = Path(temp_dir)
                zfile.extractall(temp_dir_path)
                self.validate_log(temp_dir_path, 3, 16945)

    def validate_log(self, event_log_path: Path, log_file_total: int, log_entry_total: int):
        with tempfile.TemporaryDirectory() as temp_dir:
            event_log_paths = extractor.Extractor(
                event_log_path.resolve().as_uri(), temp_dir
            ).extract()
            event_log, parsed = eventlog.EventLogBuilder(event_log_paths, temp_dir).build()

            with open(event_log) as log_fobj:
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
                        ), "Not all keys are present"
                        assert (
                            rollover_count == event["Rollover Number"]
                        ), "Rollover IDs are not contiguous and monotonically increasing"
                        rollover_count += 1

                assert rollover_count == log_file_total, "Not all log parts are present"
                assert i + 1 == log_entry_total, "Not all events are present"
                assert not parsed


if __name__ == "__main__":

    test_simple_emr_log()

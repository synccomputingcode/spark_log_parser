import tempfile
import unittest
from pathlib import Path

from spark_log_parser import eventlog


class BadEventLog(unittest.TestCase):
    def test_multiple_context_ids(self):
        event_log = Path("tests", "logs", "bad", "non-unique-context-id.zip").resolve()

        with tempfile.TemporaryDirectory(
            prefix="eventlog-%s-" % event_log.name[: -len("".join(event_log.suffixes))]
        ) as temp_dir:
            with self.assertRaises(
                ValueError, msg="Not all rollover files have the same Spark context ID"
            ):
                eventlog.EventLog(source_url=event_log.as_uri(), work_dir=temp_dir)

    def test_missing_dbc_event(self):
        event_log = Path("tests", "logs", "bad", "missing-dbc-event.zip").resolve()

        with tempfile.TemporaryDirectory(
            prefix="eventlog-%s-" % event_log.name[: -len("".join(event_log.suffixes))]
        ) as temp_dir:
            with self.assertRaises(ValueError, msg="Expected DBC event not found"):
                eventlog.EventLog(source_url=event_log.as_uri(), work_dir=temp_dir)

    def test_duplicate_log_part(self):
        event_log = Path("tests", "logs", "bad", "duplicate-part.tgz").resolve()

        with tempfile.TemporaryDirectory(
            prefix="eventlog-%s-" % event_log.name[: -len("".join(event_log.suffixes))]
        ) as temp_dir:
            with self.assertRaises(ValueError, msg="Duplicate rollover file detected"):
                eventlog.EventLog(source_url=event_log.as_uri(), work_dir=temp_dir)

    def test_missing_log_part(self):
        event_log = Path("tests", "logs", "bad", "missing-part.zip").resolve()

        with tempfile.TemporaryDirectory(
            prefix="eventlog-%s-" % event_log.name[: -len("".join(event_log.suffixes))]
        ) as temp_dir:
            with self.assertRaises(ValueError, msg="Rollover file appears to be missing"):
                eventlog.EventLog(source_url=event_log.as_uri(), work_dir=temp_dir)

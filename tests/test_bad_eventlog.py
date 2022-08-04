import tempfile
import unittest
from pathlib import Path
from zipfile import ZipFile

from spark_log_parser.eventlog import EventLogBuilder


class BadEventLog(unittest.TestCase):
    def test_multiple_context_ids(self):
        event_log = Path("tests", "logs", "bad", "non-unique-context-id.zip").resolve()

        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaises(
                ValueError, msg="Not all rollover files have the same Spark context ID"
            ):
                EventLogBuilder(event_log.as_uri(), temp_dir).build()

    def test_missing_dbc_event(self):
        event_log = Path("tests", "logs", "bad", "missing-dbc-event.zip").resolve()

        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaises(ValueError, msg="Expected DBC event not found"):
                EventLogBuilder(event_log.as_uri(), temp_dir).build()

    def test_duplicate_log_part(self):
        event_log = Path("tests", "logs", "bad", "duplicate-part.tgz").resolve()

        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaises(ValueError, msg="Duplicate rollover file detected"):
                EventLogBuilder(event_log.as_uri(), temp_dir).build()

    def test_missing_log_part(self):
        event_log = Path("tests", "logs", "bad", "missing-part.zip").resolve()

        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaises(ValueError, msg="Rollover file appears to be missing"):
                EventLogBuilder(event_log.as_uri(), temp_dir).build()

    def test_missing_first_part(self):
        event_log = Path("tests", "logs", "bad", "missing-first-part.zip").resolve()

        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaises(ValueError, msg="Rollover file appears to be missing"):
                EventLogBuilder(event_log.as_uri(), temp_dir).build()

    def test_only_non_first_part(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            with ZipFile(Path("tests", "logs", "bad", "missing-first-part.zip")) as zfile:
                zfile.extract(
                    [zinfo for zinfo in zfile.infolist() if not zinfo.is_dir()][0], temp_dir
                )

            with self.assertRaises(ValueError, msg="Rollover file appears to be missing"):
                EventLogBuilder(Path(temp_dir).as_uri(), temp_dir).build()

    def test_empty_log_dir(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaises(ValueError, msg="No log files found"):
                EventLogBuilder(Path(temp_dir).as_uri(), temp_dir).build()

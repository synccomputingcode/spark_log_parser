import tempfile
import unittest
from pathlib import Path
from zipfile import ZipFile

from spark_log_parser import eventlog, extractor


class BadEventLog(unittest.TestCase):

    def check_value_error(self, event_log_path, msg):

        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaises(ValueError, msg=msg):
                event_log_paths = extractor.Extractor(event_log_path.as_uri(), temp_dir).extract()
                eventlog.EventLogBuilder(event_log_paths, temp_dir).build()
            
    def test_multiple_context_ids(self):
        event_log = Path("tests", "logs", "bad", "non-unique-context-id.zip").resolve()
        self.check_value_error(event_log, "Not all rollover files have the same Spark context ID")

    def test_missing_dbc_event(self):
        event_log = Path("tests", "logs", "bad", "missing-dbc-event.zip").resolve()
        self.check_value_error(event_log, "Expected DBC event not found")

    def test_duplicate_log_part(self):
        event_log = Path("tests", "logs", "bad", "duplicate-part.tgz").resolve()
        self.check_value_error(event_log, "Duplicate rollover file detected")

    def test_missing_log_part(self):
        event_log = Path("tests", "logs", "bad", "missing-part.zip").resolve()
        self.check_value_error(event_log, "Rollover file appears to be missing")

    def test_missing_first_part(self):
        event_log = Path("tests", "logs", "bad", "missing-first-part.zip").resolve()
        self.check_value_error(event_log, "Rollover file appears to be missing")

    def test_only_non_first_part(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            with ZipFile(Path("tests", "logs", "bad", "missing-first-part.zip")) as zfile:
                zfile.extract(
                    [zinfo for zinfo in zfile.infolist() if not zinfo.is_dir()][0], temp_dir
                )

            self.check_value_error(Path(temp_dir), "Rollover file appears to be missing")


    def test_empty_log_dir(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            self.check_value_error(Path(temp_dir), "No log files found")

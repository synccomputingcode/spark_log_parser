import json
import tempfile
from pathlib import Path

import pandas as pd

from spark_log_parser.parsing_models.exceptions import LogSubmissionException


class EventLogBuilder:
    def __init__(
        self,
        event_log_paths: list[Path] | list[str],
        work_dir: Path | str,
    ):
        self.event_log_paths = self._validate_event_log_paths(event_log_paths)
        self.work_dir = self._validate_work_dir(work_dir)

    def _validate_event_log_paths(self, event_log_paths: list[Path] | list[str]) -> list[Path]:
        return [Path(x) for x in event_log_paths]

    def _validate_work_dir(self, work_dir: Path | str) -> Path:
        work_dir_path = work_dir if isinstance(work_dir, Path) else Path(work_dir)
        if not work_dir_path.is_dir():
            raise ValueError("Path is not a directory")

        return work_dir_path

    def build(self) -> tuple[Path, bool]:

        if not self.event_log_paths:
            raise LogSubmissionException(
                error_message="No Spark eventlogs were found in submission"
            )

        self.event_log, self.parsed = self._get_event_log(self.event_log_paths)

        return self.event_log, self.parsed

    def _get_event_log(self, paths: list[Path]) -> tuple[Path, bool]:

        log_files = []
        rollover_dat = []
        parsed = False
        for path in paths:
            try:  # Test if it is a raw log
                with open(path) as fobj:
                    line = json.loads(fobj.readline())
                    if "Event" in line:
                        log_files.append(path)
                        if line["Event"] == "DBCEventLoggingListenerMetadata":
                            rollover_dat.append(
                                (line["Rollover Number"], line["SparkContext Id"], path)
                            )
                    else:
                        raise ValueError

            except ValueError:
                try:  # Test if it is a parsed log
                    with open(path) as fobj:
                        data = json.load(fobj)
                        if "jobData" in data:
                            log_files.append(path)
                            parsed = True
                except ValueError:
                    continue

        if len(log_files) > 1 and parsed:
            raise LogSubmissionException("A parsed log file was submitted with other log files")

        if rollover_dat:
            if len(log_files) > len(rollover_dat):
                raise LogSubmissionException(
                    error_message="Rollover logs were detected, but not all files had rollover properties"
                )

            return self._concat(rollover_dat), False

        if len(log_files) > 1:
            raise LogSubmissionException(
                error_message="Multiple files detected without log rollover properties"
            )

        return log_files[0], parsed

    def _concat(self, rollover_dat: list[tuple[str, str, str]]) -> Path:
        rollover_df = (
            pd.DataFrame(rollover_dat, columns=["rollover_index", "context_id", "path"])
            .sort_values("rollover_index")
            .reset_index()
        )

        if not len(rollover_df.context_id.unique()) == 1:
            raise LogSubmissionException(
                error_message="Not all rollover log files have the same Spark context ID"
            )

        diffs = rollover_df.rollover_index.diff()

        if any(diffs > 1) or rollover_df.rollover_index[0] > 0:
            raise LogSubmissionException(error_message="One or more rollover logs is missing")

        if any(diffs < 1):
            raise LogSubmissionException(error_message="Duplicate rollover log file detected")

        event_log = Path(tempfile.mkstemp(suffix="-concatenated.json", dir=str(self.work_dir))[1])
        with open(event_log, "w") as fobj:
            for path in rollover_df.path:
                with open(path) as part_fobj:
                    for line in part_fobj:
                        fobj.write(line)

        return event_log

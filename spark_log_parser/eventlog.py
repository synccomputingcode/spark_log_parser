import json
import tempfile
from pathlib import Path

import pandas as pd


class EventLogBuilder:
    def __init__(
        self,
        event_log_paths: list[Path] | list[str],
        work_dir: Path | str,
    ):

        self.event_log_paths = self._validate_event_log_paths(event_log_paths)
        self.work_dir = self._validate_work_dir(work_dir)

    def _validate_event_log_paths(self, event_log_paths: Path | str) -> Path:
        return [Path(x) for x in event_log_paths]

    def _validate_work_dir(self, work_dir: Path | str) -> Path:
        work_dir_path = work_dir if isinstance(work_dir, Path) else Path(work_dir)
        if not work_dir_path.is_dir():
            raise ValueError("Path is not a directory")

        return work_dir_path

    def build(self) -> Path:

        if not self.event_log_paths:
            raise ValueError("No files found")

        self.event_log = self._get_event_log(self.event_log_paths)

        return self.event_log

    def _get_event_log(self, paths: list[Path]) -> Path:
        log_files = []
        rollover_dat = []
        for path in paths:
            with open(path) as fobj:
                try:
                    line = json.loads(fobj.readline())
                except ValueError:
                    continue
                if "Event" in line:
                    log_files.append(path)
                    if line["Event"] == "DBCEventLoggingListenerMetadata":
                        rollover_dat.append(
                            (line["Rollover Number"], line["SparkContext Id"], path)
                        )

        if rollover_dat:
            if len(log_files) > len(rollover_dat):
                raise ValueError("No rollover properties found in log file")

            return self._concat(rollover_dat)

        if len(log_files) > 1:
            raise ValueError("No rollover properties found in log file")

        return log_files[0]

    def _concat(self, rollover_dat: list[tuple[str, str, str]]) -> Path:
        rollover_df = (
            pd.DataFrame(rollover_dat, columns=["rollover_index", "context_id", "path"])
            .sort_values("rollover_index")
            .reset_index()
        )

        if not len(rollover_df.context_id.unique()) == 1:
            raise ValueError("Not all rollover log files have the same Spark context ID")

        diffs = rollover_df.rollover_index.diff()

        if any(diffs > 1) or rollover_df.rollover_index[0] > 0:
            raise ValueError("Rollover log file appears to be missing")

        if any(diffs < 1):
            raise ValueError("Duplicate rollover log file detected")

        event_log = Path(tempfile.mkstemp(suffix="-concatenated.json", dir=str(self.work_dir))[1])
        with open(event_log, "w") as fobj:
            for path in rollover_df.path:
                with open(path) as part_fobj:
                    for line in part_fobj:
                        fobj.write(line)

        return event_log

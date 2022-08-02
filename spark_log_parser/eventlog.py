import json
import tempfile
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd

from spark_log_parser.extractor import Extractor


class EventLogBuilder:
    ALLOWED_SCHEMES = {"https", "s3", "file"}

    def __init__(self, source_url: str, work_dir: Path | str, s3_client=None):
        self.source_url = self._validate_url(source_url)
        self.work_dir = self._validate_work_dir(work_dir)
        self.s3_client = s3_client
        self.file_total = 0
        self.size_total = 0

    def _validate_url(self, url: str):
        parsed_url = urlparse(url)
        if parsed_url.scheme not in self.ALLOWED_SCHEMES:
            raise ValueError(
                "URL scheme '%s' is not one of {'%s'}"
                % (parsed_url.scheme, "', '".join(self.ALLOWED_SCHEMES))
            )

        return parsed_url

    def _validate_work_dir(self, work_dir: Path):
        work_dir_path = work_dir if isinstance(work_dir, Path) else Path(work_dir)
        if not work_dir_path.is_dir():
            raise ValueError("Path is not a directory")

        return work_dir_path

    def build(self) -> "EventLogBuilder":
        event_logs = Extractor(self.source_url, self.work_dir, self.s3_client).extract()

        # name = Path(self._basename())

        self.event_log = self._concat(
            event_logs,
            Path(tempfile.mkstemp(suffix="-concatenated.json", dir=str(self.work_dir))[1]),
        )

        return self.event_log

    def _concat(self, event_logs: list[Path], event_log: Path) -> Path:
        if len(event_logs) == 1:
            return event_logs[0]

        dat = []
        for log in event_logs:
            with open(log) as log_file:
                try:
                    line = json.loads(log_file.readline())
                except ValueError:
                    continue  # Maybe a Databricks pricing file
                if line["Event"] == "DBCEventLoggingListenerMetadata":
                    dat.append((line["Rollover Number"], line["SparkContext Id"], log))
                else:
                    raise ValueError("Expected DBC event not found")

        df = pd.DataFrame(dat, columns=["rollover_index", "context_id", "path"]).sort_values(
            "rollover_index"
        )

        self._validate_rollover_logs(df)

        with open(event_log, "w") as fobj:
            for path in df.path:
                with open(path) as part_fobj:
                    for line in part_fobj:
                        fobj.write(line)

        return event_log

    def _validate_rollover_logs(self, df: pd.DataFrame):
        if not len(df.context_id.unique()) == 1:
            raise ValueError("Not all rollover files have the same Spark context ID")

        diffs = df.rollover_index.diff()[1:]

        if any(diffs > 1):
            raise ValueError("Rollover file appears to be missing")

        if any(diffs < 1):
            raise ValueError("Duplicate rollover file detected")

    def _basename(self):
        if self.source_url.scheme in {"https", "s3"}:
            return self.source_url.path.split("/")[-1]
        elif self.source_url.scheme == "file":
            return Path(self.source_url.path).name

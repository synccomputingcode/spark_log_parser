import json
from pathlib import Path
from typing import Optional

import pandas as pd
from pydantic import BaseModel, ValidationError, root_validator, stricturl

from spark_log_parser.extractor import Extractor

AllowedURL = stricturl(
    host_required=False, tld_required=False, allowed_schemes={"https", "s3", "file"}
)


class EventLog(BaseModel):
    source_url: Optional[AllowedURL] = None
    work_dir: Optional[Path] = None
    event_log: Optional[Path] = None

    @root_validator()
    def validate_event_log(cls, values):
        if not values["event_log"]:
            if values["source_url"] and values["work_dir"]:
                return vars(
                    EventLogBuilder(
                        values["source_url"],
                        values["work_dir"],
                    ).build()
                )
            raise ValidationError("source_url and work_dir must be set if event_log isn't")

        return values


class EventLogBuilder:
    def __init__(self, source_url: AllowedURL, work_dir: Path):
        self.source_url = source_url
        self.work_dir = work_dir
        self.file_total = 0
        self.size_total = 0

    def build(self) -> "EventLogBuilder":
        event_logs = Extractor(self.source_url, self.work_dir).extract()

        name = Path(self._basename())

        self.event_log = self._concat(
            event_logs,
            self.work_dir.joinpath(
                name.name[: -len("".join(name.suffixes))] + "-concatenated.json"
            ),
        )

        return self

    def _concat(self, event_logs: list[Path], event_log: Path):
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

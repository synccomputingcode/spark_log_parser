import argparse
import logging
import sys
import tempfile
from pathlib import Path
from urllib.parse import unquote

import spark_log_parser

logging.captureWarnings(True)

from spark_log_parser.eventlog import EventLogBuilder  # noqa: E402
from spark_log_parser.extractor import Extractor, ExtractThresholds  # noqa: E402
from spark_log_parser.parsing_models.application_model_v2 import sparkApplication  # noqa: E402

logger = logging.getLogger("spark_log_parser")


def main():
    parser = argparse.ArgumentParser("spark-log-parser")
    parser.add_argument(
        "-l", "--log-file", required=True, type=Path, help="path to event log file or directory"
    )
    parser.add_argument(
        "-r",
        "--result-dir",
        required=True,
        type=Path,
        help="path to directory in which to save the parsed log",
    )
    parser.add_argument(
        "--version", action="version", version="%(prog)s " + spark_log_parser.__version__
    )
    args = parser.parse_args()

    if not args.result_dir.is_dir():
        logger.error("%s is not a directory", args.result_dir)
        sys.exit(1)

    print("\n" + "*" * 12 + " Running the Log Parser for Spark Predictor " + "*" * 12 + "\n")
    print("--Processing log file: " + str(args.log_file))

    with tempfile.TemporaryDirectory() as work_dir:

        event_log_paths = Extractor(
            unquote(args.log_file.resolve().as_uri()), work_dir, thresholds=ExtractThresholds(size=20000000000)
        ).extract()

        event_log = EventLogBuilder(event_log_paths, work_dir).build()
        app = sparkApplication(eventlog=str(event_log))

    if args.log_file.suffixes:
        result_path = args.result_dir.joinpath(
            "parsed-" + args.log_file.name[: -len("".join(args.log_file.suffixes))]
        )
    else:
        result_path = args.result_dir.joinpath("parsed-" + args.log_file.name)

    app.save(str(result_path))

    print(f"--Result saved to: {result_path}.json")

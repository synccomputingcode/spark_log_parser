import argparse
import logging
import shutil
import sys
import tempfile
from pathlib import Path
from urllib.parse import unquote

import spark_log_parser

logging.captureWarnings(True)

from spark_log_parser.eventlog import EventLogBuilder  # noqa: E402
from spark_log_parser.extractor import Extractor, ExtractThresholds  # noqa: E402
from spark_log_parser.parsing_models.application_model_v2 import SparkApplication, \
    create_spark_application  # noqa: E402

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

    if args.log_file.suffixes:
        result_path = args.result_dir.joinpath(
            "parsed-" + args.log_file.name[: -len("".join(args.log_file.suffixes))]
        )
    else:
        result_path = args.result_dir.joinpath("parsed-" + args.log_file.name)

    with tempfile.TemporaryDirectory() as work_dir:

        event_log_paths = Extractor(
            unquote(args.log_file.resolve().as_uri()),
            work_dir,
            thresholds=ExtractThresholds(size=20000000000),
        ).extract()

        event_log, parsed = EventLogBuilder(event_log_paths, work_dir).build()

        if not parsed:
            app = create_spark_application(spark_eventlog_path=str(event_log))
            app.save(str(result_path))
        else:
            print("--Input log was already parsed")
            shutil.copyfile(event_log, str(result_path) + ".json")

    print(f"--Result saved to: {result_path}.json")

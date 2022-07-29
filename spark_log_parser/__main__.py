import logging

logging.captureWarnings(True)

import tempfile
from spark_log_parser.eventlog import EventLog

from spark_log_parser.parsing_models.application_model_v2 import sparkApplication

import os
import argparse
from pathlib import Path
import sys

logger = logging.getLogger("spark_log_parser")

parser = argparse.ArgumentParser("spark_log_parser")
parser.add_argument("-l", "--log-file", required=True, type=Path, help="path to event log")
parser.add_argument("-r", "--result-dir", required=True, help="path to directory in which to save parsed logs")
args = parser.parse_args()  

if not os.path.isdir(args.result_dir):
    logger.error("%s is not a directory", args.result_dir)
    sys.exit(1)

print("\n" + "*" * 12 + " Running the Log Parser for Spark Predictor " + "*" * 12 + "\n")
print("--Processing log file: " + str(args.log_file))

with tempfile.TemporaryDirectory() as work_dir:
    event_log = EventLog(source_url=args.log_file.resolve().as_uri(), work_dir=work_dir)
    app = sparkApplication(eventlog=str(event_log.event_log))

result_path = os.path.join(args.result_dir, "parsed-" + args.log_file.name[:-len("".join(args.log_file.suffixes))])
app.save(result_path)

print(f"--Result saved to: {result_path}.json")

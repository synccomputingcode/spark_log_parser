import logging

logging.captureWarnings(True)

from spark_log_parser.parsing_models.application_model_v2 import sparkApplication

import os
import argparse
import sys

logger = logging.getLogger("spark_log_parser")
            
if __name__ == '__main__':
    parser = argparse.ArgumentParser("spark_log_parser")
    parser.add_argument("-l", "--log-file", required=True, help="path to event log")
    parser.add_argument("-r", "--result-dir", required=True, help="path to directory in which to save parsed logs")
    args = parser.parse_args()  

    print("\n" + "*" * 12 + " Running the Log Parser for Spark Predictor " + "*" * 12 + "\n")

    log_path = os.path.abspath(args.log_file)

    if not os.path.isdir(args.result_dir):
        logger.error("%s is not a directory", args.result_dir)
        sys.exit(1)
    
    print("\n--Processing log file: " + log_path)

    log_name = os.path.basename(log_path)
    result_path = os.path.join(args.result_dir, 'parsed-' + log_name)

    if os.path.exists(result_path):
        os.remove(result_path)

    appobj = sparkApplication(eventlog=log_path)
    appobj.save(result_path)

    print(f"--Log directory saved to: {result_path}")

# log_parser
The **Parser for Apache Spark** parses unmodified Apache Spark History Server Event logs.

Parsed logs contain metadata pertaining to your Apache Spark application execution. Particularly, the runtime for a task, the amount of data read & written, the amount of memory used, etc. These logs do not contain
sensitive information such as the data that your Apache Spark application is processing. Below is an example of the output of the log parser
![Output of Log Parser](docs/output.png)

# Installation
Clone this repo to the desired directory.

# Getting Started
### Step 0: Generate the appropriate Apache Spark History Server Event log
If you have not already done so, complete the [instructions](https://github.com/synccomputingcode/client_tools#step-2-retrieve-emr-spark-logs-and-upload-into-autotuner-step-2 to download the Apache Spark event log.

### Step 1: Parse the log to strip away sensitive information
1. To process a log file, execute the parse.py script in the sync_parser folder, and provide a
log file destination with the -d flag.

    `python3 sync_parser/parse.py -d [log file location]`

    The parsed file `parsed-[log file name]` will appear in the results directory.


2. Send Sync Computing the parsed log

Email Sync Computing (or upload to the Sync Auto-tuner) the parsed event log.

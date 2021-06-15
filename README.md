# spark_log_parser
The Spark log parser parses unmodified Spark output logs.

Parsed logs contain metadata pertaining to your Spark application execution. Particularly, the runtime for a task, the amount of data read & written, the amount of memory used, etc. These logs do not contain
sensitive information such as the data that your Spark application is processing. Below is an example of the output of the log parser
![Output of Log Parser](https://github.com/synccomputingcode/spark_log_parser/blob/main/docs/output.png)

# Installation
Clone this repo to the desired directory.

# Getting Started
### Step 0: Generate the appropriate Apache Spark EMR log
If you have not already done so, complete the [instructions](https://github.com/synccomputingcode/spark_log_parser/blob/main/docs/event_log_download.pdf) to download the spark event log.

### Step 1: Parse the log to strip away sensitive information
1. To process a log file, execute the parse.py script in the sync_parser folder, and provide a
log file destination with the -d flag.

    `python3 sync_parser/parse.py -d [log file location]`

    The parsed file `[log file name].spk` will appear in the sync_parser/results directory.
    
    To re-process and overwrite a previously generated parsed log add the -o flag:

    `python3 sync_parser/parse.py -d [log file location] -o`

3. Send Sync Computing the parsed log

    The parsed file `[log file name].spk` will appear in the sync_parser/results directory. Email
your contact at Sync Computing the parsed file.
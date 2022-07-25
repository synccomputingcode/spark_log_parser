# Spark Log Parser
The **Parser for Apache Spark** parses unmodified Apache Spark History Server Event logs.

Parsed logs contain metadata pertaining to your Apache Spark application execution. Particularly, the runtime for a task, the amount of data read & written, the amount of memory used, etc. These logs do not contain
sensitive information such as the data that your Apache Spark application is processing. Below is an example of the output of the log parser.
![Output of Log Parser](docs/output.png)

# Installation
Install the package in this repo to your Python 3 environment, e.g.
```shell
pip3 install https://github.com/synccomputingcode/spark_log_parser/archive/v0.0.1.tar.gz
```

# Parsing your Spark logs
### Step 0: Generate the appropriate Apache Spark History Server Event log
If you have not already done so, complete the [instructions](https://github.com/synccomputingcode/user_documentation/wiki#accessing-autotuner-input-data) to download the Apache Spark event log.

### Step 1: Parse the log to strip away sensitive information
1. To process a log file, execute the parse.py script in the sync_parser folder, and provide a
log file destination with the -l flag.

    ```shell
    python3 -m spark_log_parser -l <log file location>
    ```

    The parsed file `parsed-<log file name>` will appear in the results directory.


2. Send Sync Computing the parsed log

    Email Sync Computing (or upload to the Sync Auto-tuner) the parsed event log.

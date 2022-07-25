import logging

logger = logging.getLogger("ParserExceptionLogger")

class ParserErrorMessages():

    SPARK_CONFIG_GENERIC_MESSAGE = (

        "Some configurations were detected that are not yet supported by the Sync Autotuner. " +
        "If you would like to try again, please make the following configuration changes and " +
        "rerun your application:"
    )

    MISSING_EVENT_JOB_START = (
        "Event SparkListenerJobStart was missing for the following jobs: ")
    MISSING_EVENT_JOB_END = (
        "Event SparkListenerJobEnd was missing for the following jobs: ")

    MISSING_EVENT_STAGE_SUBMIT = (
        "Event SparkListenerStageSubmitted was missing for the following stages: ")
    MISSING_EVENT_STAGE_COMPLETE = (
        "Event SparkListenerStageCompleted was missing for the following stages: ")

    MISSING_EVENT_GENERIC_MESSAGE = (
        "Some Spark Listener Event data is missing from the eventlog related to: ")

    MISSING_EVENT_EXPLANATION = (
        "This Event data is necessary for the Sync Autotuner. " +
        "Events may be missing for a number of reasons including " +
        "-- (1) The application did not complete successfully " +
        "-- (2) If these are rollover logs, there may be one or more missing logs " +
        "-- (3) Spark Listener communication failures. " +
        "There are some steps that can help mitigate these issues " +
        "-- (1) Ensure the SparkContext closes correctly at the end of your application, e.g. " +
            "by using sc.stop() " +
        "-- (2) If you submitted a rollover log set, ensure that all rollover logs for the " +
            "application were submitted " +
        "-- (3) Resubmit the next log produced with this application."
    )

    SUPPORT_MESSAGE = (
        "If you have questions or would like assistance in resolving the issue " +
        "please contact our support at support@synccomputing.com.")

class ParserErrorTypes():

    SPARK_CONFIG_ERROR = "Invalid Spark Configuration Error"
    MISSING_EVENT_ERROR = 'Event missing from Spark eventlog'

class ParserErrorCodes():

    SPARK_CONFIG_ERROR = 2001
    SPARK_EVENT_ERROR = 2002
import logging
from pprint import pformat
from deepdiff import DeepDiff

logging.basicConfig()
logging.root.setLevel(logging.INFO)

# All the top-level keys that we would expect to be present in the JSON representation of a parsed SparkApplication
PARSED_KEYS = [
    "accumData",
    "executors",
    "jobData",
    "metadata",
    "sqlData",
    "stageData",
    "taskData",
]

# In parsed log files (i.e. SparkApplication.to_dict()), this is the set of top-level keys for which we would expect
# members to not necessarily always be ordered the same way
UNORDERED_PARSED_KEYS = [
    "accumData",
    "sqlData",
    "executors"
]

APP_NAME_INCORRECT_MESSAGE = "Application name does not match expected value"
PARSED_KEYS_MISSING_MESSAGE = "Not all keys present in parsed eventlog"


def assert_all_files_equivalent(parsed_files):
    """
    Given a list of already-parsed eventlog file dictionaries (i.e. SparkApplication.to_dict()), assert that all
    dictionaries are meaningfully the same, ignoring ordering of keys as is applicable
    """
    [first, *rest] = parsed_files
    for curr in rest:
        for key, curr_value in curr.items():
            first_value = first[key]

            # This is much faster than a DeepDiff, and since we expect these values to match most of the time, this
            #  should help speed up tests in the common-case that things are working as expected
            if curr_value == first_value:
                continue

            diff = DeepDiff(curr_value, first_value)
            # If we get a diff, we should double-check that that diff isn't just due to ordering differences, which may
            #  not be an issue at all. This should help more readily pinpoint what the actual issue in any detected diff
            #  may be
            if diff:
                diff_ignoring_order = DeepDiff(curr_value, first_value, ignore_order=True)

            if diff and not diff_ignoring_order and key not in UNORDERED_PARSED_KEYS:
                raise ValueError(f"Detected an ordering difference for key: {key} in parsed log files.\n" +
                                 f"If this is expected, please update UNORDERED_PARSED_KEYS. Otherwise, please " +
                                 "fix this diff.\n"
                                 f"{pformat(diff)}")

            if diff and diff_ignoring_order:
                raise ValueError(f"Detected a difference not due to ordering for key: {key} in parsed log files\n" +
                                 f"{pformat(diff_ignoring_order)}")


import io
import json
import os
import tempfile
from pathlib import Path
from urllib.parse import urlparse
from zipfile import ZipFile

import pytest
import boto3 as boto
from botocore.stub import ANY, Stubber


# @pytest.mark.parametrize(
#     "event_log_url",
#     [
#         "s3://sync-test-artifacts/airlinedelay/jb-42K1E16/emr.zip",
#         "s3://sync-test-artifacts/airlinedelay/jb-42K1E16/",
#     ],
# )
# @pytest.mark.parametrize(
#     "event_log_file_archive,event_log_s3_dir",
#     [(Path("tests", "logs", "emr.zip"), "airlinedelay/jb-42K1E16/")],
# )
# def test_emr_log_from_s3(event_log_url, event_log_file_archive, event_log_s3_dir):

import io
import os
from pathlib import Path
from urllib.parse import urlparse
from zipfile import ZipFile

import pytest
import boto3 as boto
from botocore.stub import Stubber
from botocore.response import StreamingBody

from spark_log_parser.loaders import AbstractFileDataLoader
from spark_log_parser.parsing_models.application_model_v2 import create_spark_application
from tests import ROOT_DIR

BOTO_CLIENT_STUB_TARGET = "spark_log_parser.loaders.s3.S3_CLIENT"
S3_GET_OBJECT_CONTENT_TYPE = "application/octet-stream"


def assert_file_parsed_properly(eventlog_url, eventlog_file, eventlog_s3_dir, create_spark_app_fn, mocker):
    parsed_event_log_url = urlparse(eventlog_url)

    s3_bucket = parsed_event_log_url.netloc
    s3_prefix = parsed_event_log_url.path.lstrip("/")

    s3 = boto.client("s3")
    stubber = Stubber(s3)
    s3_key = eventlog_s3_dir + eventlog_file.name
    s3_object_size = eventlog_file.stat().st_size
    stubber.add_response(
        "list_objects_v2",
        {"Contents": [{"Key": s3_key, "Size": s3_object_size}]},
        {"Bucket": s3_bucket, "Prefix": s3_prefix},
    )

    with open(eventlog_file, "rb") as fobj:
        file_content = fobj.read()
        file_bytes = io.BytesIO(file_content)
        file_size = len(file_content)
        expected_params = {"Bucket": s3_bucket, "Key": s3_key}
        stubber.add_response(
            "get_object",
            {
                "ContentType": S3_GET_OBJECT_CONTENT_TYPE,
                "ContentLength": file_size,
                "Body": StreamingBody(file_bytes, file_size)
            },
            expected_params,
        )

    with stubber:
        mocker.patch(BOTO_CLIENT_STUB_TARGET, new=s3)
        spark_app = create_spark_app_fn()

    stubber.assert_no_pending_responses()
    metadata = spark_app.metadata
    assert spark_app is not None
    assert metadata['application_info']['spark_version'] is not None


@pytest.mark.parametrize(
    "event_log_url",
    [
        "s3://sync-test-artifacts/airlinedelay/jb-42K1E16/emr.zip",
        "s3://sync-test-artifacts/airlinedelay/jb-42K1E16/",
    ],
)
@pytest.mark.parametrize(
    "event_log_file_archive,event_log_s3_dir",
    [(Path(ROOT_DIR, "logs", "emr.zip"), "airlinedelay/jb-42K1E16/")],
)
def test_emr_log_from_s3(event_log_url, event_log_file_archive, event_log_s3_dir, mocker):
    def create_app():
        return create_spark_application(path=str(event_log_url))

    assert_file_parsed_properly(event_log_url, event_log_file_archive, event_log_s3_dir, create_app, mocker)


@pytest.mark.parametrize(
    "event_log_url",
    [
        "s3://sync-test-artifacts/foo/bar-baz/similarity_parsed.json.gz",
        "s3://sync-test-artifacts/foo/bar-baz/",
    ],
)
@pytest.mark.parametrize(
    "event_log_file_archive,event_log_s3_dir",
    [(Path(ROOT_DIR, "logs", "similarity_parsed.json.gz"), "/foo/bar-baz/")],
)
def test_parsed_log_from_s3(event_log_url, event_log_file_archive, event_log_s3_dir, mocker):
    def create_app():
        return create_spark_application(path=str(event_log_url))

    assert_file_parsed_properly(event_log_url, event_log_file_archive, event_log_s3_dir, create_app, mocker)


@pytest.mark.parametrize(
    "event_log_url",
    [
        "s3://sync-test-artifacts/foo/bar-baz/databricks.json",
        "s3://sync-test-artifacts/foo/bar-baz/",
    ],
)
@pytest.mark.parametrize(
    "event_log_file_archive,event_log_s3_dir",
    [(Path(ROOT_DIR, "logs", "databricks.json"), "/foo/bar-baz/")],
)
def test_raw_log_from_s3(event_log_url, event_log_file_archive, event_log_s3_dir, mocker):
    def create_app():
        return create_spark_application(path=str(event_log_url))

    assert_file_parsed_properly(event_log_url, event_log_file_archive, event_log_s3_dir, create_app, mocker)


@pytest.mark.parametrize(
    "event_log_url",
    ["s3://sync-test-artifacts/airlinedelay/jb-42K1E16/", "s3://sync-test-artifacts/airlinedelay"],
)
@pytest.mark.parametrize(
    "event_log_file_archive,event_log_s3_dir",
    [(Path(ROOT_DIR, "logs", "databricks-rollover-messy.zip"), "airlinedelay/jb-42K1E16/")],
)
def test_databricks_log_from_s3_dir(event_log_url, event_log_file_archive, event_log_s3_dir, mocker):
    parsed_event_log_url = urlparse(event_log_url)
    s3_bucket = parsed_event_log_url.netloc
    s3_prefix = parsed_event_log_url.path.lstrip("/")

    s3 = boto.client("s3")
    stubber = Stubber(s3)
    with ZipFile(event_log_file_archive) as zfile:
        contents = [
            (event_log_s3_dir + "/".join(zinfo.filename.split(os.sep)), zinfo)
            for zinfo in zfile.infolist()
            if not zinfo.is_dir()
        ]
        stubber.add_response(
            "list_objects_v2",
            {"Contents": [{"Key": key, "Size": zinfo.file_size} for key, zinfo in contents]},
            {"Bucket": s3_bucket, "Prefix": s3_prefix},
        )

        for s3_key, zinfo in contents:
            if AbstractFileDataLoader.should_skip_file(zinfo.filename):
                continue

            with zfile.open(zinfo) as zobj:
                file_content = zobj.read()
                file_bytes = io.BytesIO(file_content)
                file_size = len(file_content)
                expected_params = {"Bucket": s3_bucket, "Key": s3_key}
                stubber.add_response(
                    "get_object",
                    {
                        "ContentType": S3_GET_OBJECT_CONTENT_TYPE,
                        "ContentLength": len(file_content),
                        "Body": StreamingBody(file_bytes, file_size),
                    },
                    expected_params,
                )

    with stubber:
        mocker.patch(BOTO_CLIENT_STUB_TARGET, new=s3)
        spark_app = create_spark_application(path=str(event_log_url))

    stubber.assert_no_pending_responses()

    # For this rollover log test case, we know that there are 71 jobs spread across the rollover logs. Therefore, in
    #  order to assert that we read all of them, we can just check that job_ids 0 - 71 exist in the SparkApplication's
    #  jobData. If some are missing, that likely means we missed at least one of the files
    expected_to_see = set(range(72))
    actual_job_ids = set(spark_app.jobData.index)
    assert not expected_to_see.difference(actual_job_ids)

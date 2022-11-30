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

from spark_log_parser import eventlog, extractor
from spark_log_parser.loaders import AbstractFileDataLoader
from spark_log_parser.parsing_models.application_model_v2 import create_spark_application


@pytest.mark.parametrize(
    "event_log_url",
    [
        "s3://sync-test-artifacts/airlinedelay/jb-42K1E16/emr.zip",
        "s3://sync-test-artifacts/airlinedelay/jb-42K1E16/",
    ],
)
@pytest.mark.parametrize(
    "event_log_file_archive,event_log_s3_dir",
    [(Path("tests", "logs", "emr.zip"), "airlinedelay/jb-42K1E16/")],
)
def test_emr_log_from_s3(event_log_url, event_log_file_archive, event_log_s3_dir, mocker):
    parsed_event_log_url = urlparse(event_log_url)
    s3_bucket = parsed_event_log_url.netloc
    s3_prefix = parsed_event_log_url.path.lstrip("/")

    s3 = boto.client("s3")
    stubber = Stubber(s3)
    s3_key = event_log_s3_dir + event_log_file_archive.name
    s3_object_size = event_log_file_archive.stat().st_size
    stubber.add_response(
        "list_objects_v2",
        {"Contents": [{"Key": s3_key, "Size": s3_object_size}]},
        {"Bucket": s3_bucket, "Prefix": s3_prefix},
    )

    with open(event_log_file_archive, "rb") as fobj:
        file_content = fobj.read()
        expected_params = {"Bucket": s3_bucket, "Key": s3_key}
        stubber.add_response(
            "get_object",
            {
                "ContentType": "application/octet-stream",
                "ContentLength": len(file_content),
                "Body": io.BytesIO(file_content),
            },
            expected_params,
        )

    with stubber:
        mocker.patch("boto3.client", new=lambda _: s3)
        spark_app = create_spark_application(spark_eventlog_path=str(event_log_url))

    stubber.assert_no_pending_responses()
    assert spark_app is not None
    metadata = spark_app.metadata
    assert metadata['application_info']['spark_version'] is not None


@pytest.mark.parametrize(
    "event_log_url",
    ["s3://sync-test-artifacts/airlinedelay/jb-42K1E16/", "s3://sync-test-artifacts/airlinedelay"],
)
@pytest.mark.parametrize(
    "event_log_file_archive,event_log_s3_dir",
    [(Path("tests", "logs", "databricks-rollover-messy.zip"), "airlinedelay/jb-42K1E16/")],
)
def test_databricks_log_from_s3_dir(event_log_url, event_log_file_archive, event_log_s3_dir, mocker):
    parsed_event_log_url = urlparse(event_log_url)
    s3_bucket = parsed_event_log_url.netloc
    s3_prefix = parsed_event_log_url.path.lstrip("/")

    with tempfile.TemporaryDirectory() as temp_dir:
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
                    expected_params = {"Bucket": s3_bucket, "Key": s3_key}
                    stubber.add_response(
                        "get_object",
                        {
                            "ContentType": "application/octet-stream",
                            "ContentLength": len(file_content),
                            "Body": io.BytesIO(file_content),
                        },
                        expected_params,
                    )

        with stubber:
            mocker.patch("boto3.client", new=lambda _: s3)
            spark_app = create_spark_application(spark_eventlog_path=str(event_log_url))

        stubber.assert_no_pending_responses()
        assert False
        # print(spark_app)

        # with open(event_log) as log_fobj:
        #     event = json.loads(log_fobj.readline())
        #     log_fobj.seek(0)
        #
        #     rollover_count = 0
        #     for i, event_str in enumerate(log_fobj):
        #         try:
        #             event = json.loads(event_str)
        #             if "Rollover Number" in event:
        #                 assert all(
        #                     key in event
        #                     for key in [
        #                         "Event",
        #                         "Spark Version",
        #                         "Timestamp",
        #                         "Rollover Number",
        #                         "SparkContext Id",
        #                     ]
        #                 ), "Not all keys are present"
        #                 assert (
        #                     rollover_count == event["Rollover Number"]
        #                 ), "Rollover IDs are not contiguous and monotonically increasing"
        #                 rollover_count += 1
        #         except Exception as exc:
        #             raise ValueError("Problem with line %d: %s" % (i, event_str), exc)
        #
        #     assert rollover_count == 3, "Not all log parts are present"
        #     assert i + 1 == 16945, "Not all events are present"

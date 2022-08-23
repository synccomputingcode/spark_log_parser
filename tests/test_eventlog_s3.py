import io
import json
import os
import tempfile
from pathlib import Path
from urllib.parse import urlparse
from zipfile import ZipFile

import boto3 as boto
import pytest
from botocore.stub import ANY, Stubber

from spark_log_parser import eventlog, extractor


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
def test_emr_log_from_s3(event_log_url, event_log_file_archive, event_log_s3_dir):
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
    stubber.add_response(
        "head_object",
        {"ContentLength": s3_object_size, "ContentType": "application/octet-stream"},
        {"Bucket": s3_bucket, "Key": s3_key},
    )

    with open(event_log_file_archive, "rb") as fobj:
        chunk_size = boto.s3.transfer.TransferConfig().multipart_chunksize
        chunk = fobj.read(chunk_size)
        while chunk:
            if s3_object_size > chunk_size:
                expected_params = {"Bucket": s3_bucket, "Key": s3_key, "Range": ANY}
            else:
                expected_params = {"Bucket": s3_bucket, "Key": s3_key}
            stubber.add_response(
                "get_object",
                {
                    "ContentLength": len(chunk),
                    "ContentType": "application/octet-stream",
                    "Body": io.BytesIO(chunk),
                },
                expected_params,
            )

            chunk = fobj.read(chunk_size)

    with tempfile.TemporaryDirectory() as temp_dir, stubber:
        event_log_paths = extractor.Extractor(event_log_url, temp_dir, s3).extract()
        event_log =  eventlog.EventLogBuilder(event_log_paths, temp_dir).build()


        with open(event_log) as log_fobj:
            event = json.loads(log_fobj.readline())

    assert all(key in event for key in ["Event", "Spark Version"]), "All keys are present"

    assert event["Event"] == "SparkListenerLogStart", "Expected first event is present"


@pytest.mark.parametrize(
    "event_log_url",
    ["s3://sync-test-artifacts/airlinedelay/jb-42K1E16/", "s3://sync-test-artifacts/airlinedelay"],
)
@pytest.mark.parametrize(
    "event_log_file_archive,event_log_s3_dir",
    [(Path("tests", "logs", "databricks-rollover-messy.zip"), "airlinedelay/jb-42K1E16/")],
)
def test_databricks_log_from_s3_dir(event_log_url, event_log_file_archive, event_log_s3_dir):
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
                with zfile.open(zinfo) as zobj:
                    stubber.add_response(
                        "head_object",
                        {
                            "ContentLength": zinfo.file_size,
                            "ContentType": "application/octet-stream",
                        },
                        {"Bucket": s3_bucket, "Key": s3_key},
                    )

                    chunk_size = boto.s3.transfer.TransferConfig().multipart_chunksize
                    chunk = zobj.read(chunk_size)
                    while chunk:
                        if zinfo.file_size > chunk_size:
                            expected_params = {"Bucket": s3_bucket, "Key": s3_key, "Range": ANY}
                        else:
                            expected_params = {"Bucket": s3_bucket, "Key": s3_key}
                        stubber.add_response(
                            "get_object",
                            {
                                "ContentLength": len(chunk),
                                "ContentType": "application/octet-stream",
                                "Body": io.BytesIO(chunk),
                            },
                            expected_params,
                        )

                        chunk = zobj.read(chunk_size)

        with stubber:
            event_log_paths = extractor.Extractor(event_log_url, temp_dir, s3).extract()
            event_log =  eventlog.EventLogBuilder(event_log_paths, temp_dir).build()

        stubber.assert_no_pending_responses()

        with open(event_log) as log_fobj:
            event = json.loads(log_fobj.readline())
            log_fobj.seek(0)

            rollover_count = 0
            for i, event_str in enumerate(log_fobj):
                try:
                    event = json.loads(event_str)
                    if "Rollover Number" in event:
                        assert all(
                            key in event
                            for key in [
                                "Event",
                                "Spark Version",
                                "Timestamp",
                                "Rollover Number",
                                "SparkContext Id",
                            ]
                        ), "All keys are present"
                        assert (
                            rollover_count == event["Rollover Number"]
                        ), "Contiguous monotonically increasing IDs"
                        rollover_count += 1
                except Exception as exc:
                    raise ValueError("Problem with line %d: %s" % (i, event_str), exc)

            assert rollover_count == 3, "All log parts are present"
            assert i + 1 == 16945, "All events are present"

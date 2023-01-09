import abc
import asyncio
import gzip
import json
import logging
import os
import time
from collections import defaultdict
from typing import Generic, TypeVar
from urllib.parse import urlparse

import boto3
import numpy as np
import pandas as pd
from aiodataloader import DataLoader

from .application_model import ApplicationModel
from .validation_configs import ConfigValidationDatabricks, ConfigValidationEMR
from .validation_event_data import EventDataValidation
from ..loaders import ArchiveExtractionThresholds
from ..loaders.https import HTTPFileLinesDataLoader
from ..loaders.json import JSONBlobDataLoader, JSONLinesDataLoader
from ..loaders.local_file import LocalFileLinesDataLoader
from ..loaders.s3 import S3FileLinesDataLoader

logger = logging.getLogger("SparkApplication")


class SparkApplication:
    # TODO - are these booleans actually necessary? Would `spark_app.sqlData is not None` suffice?
    existsSQL: bool = False
    sqlData: pd.DataFrame = None

    existsExecutors: bool = False
    executorData: pd.DataFrame = None

    metadata: dict = {}

    # TODO - these DataFrames should be better documented
    jobData: pd.DataFrame = None
    stageDate: pd.DataFrame = None
    taskData: pd.DataFrame = None
    accumData: pd.DataFrame = None

    def __init__(self):
        return

    def to_dict(self):
        """
        Convert all dataframes into dictionaries and aggregate into a single dict
        """
        save_data = {}
        if hasattr(self, "jobData"):
            save_data["jobData"] = self.jobData.reset_index().to_dict("list")
        if hasattr(self, "stageData"):
            save_data["stageData"] = self.stageData.reset_index().to_dict("list")
        if hasattr(self, "taskData"):
            save_data["taskData"] = self.taskData.reset_index().to_dict("list")
        if hasattr(self, "accumData"):
            save_data["accumData"] = self.accumData.reset_index().to_dict("list")
        if self.existsSQL:
            save_data["sqlData"] = self.sqlData.reset_index().to_dict("list")
        if self.existsExecutors:
            save_data["executors"] = self.executorData.reset_index().to_dict("list")

        save_data["metadata"] = self.metadata
        return save_data

    @staticmethod
    def is_parsed_spark_app(data):
        if not isinstance(data, dict):
            return False

        # TODO - this should be more robust, but matches the current logic in eventlog.py
        return "jobData" in data

    def save(self, filepath=None, compress=False):
        save_data = self.to_dict()
        if (filepath is not None) and ("s3://" in filepath):
            self.save_to_s3(save_data, filepath, compress)
        else:
            self.save_to_local(save_data, filepath, compress)

    def save_to_local(self, saveDat, filepath, compress):
        if filepath is None:
            if self.spark_eventlog_path is None:
                raise Exception('No input eventlog found. Must specify "filepath".')
            inputFile = os.path.basename(os.path.normpath(self.spark_eventlog_path)).replace(
                ".gz", ""
            )
            filepath = inputFile + "-sync"

        if compress is False:
            with open(filepath + ".json", "w") as fout:
                fout.write(json.dumps(saveDat))
        elif compress is True:
            with gzip.open(filepath + ".json.gz", "w") as fout:
                fout.write(json.dumps(saveDat).encode("ascii"))
        logger.info("Saved object locally to: %s" % (filepath))

    def save_to_s3(self, saveDat, filepath, compress):
        s3 = boto3.client("s3")

        # Extract bucket and key from s3 filepath
        path = filepath.replace("s3://", "").split("/")
        bucket = path[0]
        key = ("/".join(path[1:])).lstrip("/") + ".json"

        if compress is False:
            s3.put_object(Bucket=bucket, Body=json.dumps(saveDat).encode("utf-8"), Key=key)
        else:
            dat = gzip.compress(json.dumps(saveDat).encode("utf-8"))
            s3.put_object(Bucket=bucket, Body=dat, Key=key + ".gz")

        logger.info("Saved object to cloud: %s" % (key))


SparkApplicationRawDataType = TypeVar("DataType")
SparkApplicationLoaderKey = TypeVar("SparkApplicationLoaderKey")


class AbstractSparkApplicationDataLoader(abc.ABC,
                                         Generic[SparkApplicationLoaderKey, SparkApplicationRawDataType],
                                         DataLoader):
    """
    Defines the methods that other data loaders should implement in order to appropriately construct
    some SparkApplication. The order in which these methods are called is defined in construct_spark_application.
    """

    @abc.abstractmethod
    async def load_raw_datas(self, keys: list[SparkApplicationLoaderKey]) -> list[SparkApplicationRawDataType]:
        """
        Implementors of this method should return the data that will be the "source-of-truth", i.e. that data from
        which this concrete class will be constructing the SparkApplication.
        """

    @abc.abstractmethod
    def init_spark_application(self, raw_data: SparkApplicationRawDataType) -> SparkApplication:
        """
        Allows subclasses to provide their own instance of SparkApplication (or some sub-class)
        """
        return SparkApplication()

    @abc.abstractmethod
    def compute_sql_info(self, raw_data: SparkApplicationRawDataType, spark_app: SparkApplication) -> SparkApplication:
        """
        This method is responsible for setting the following fields on spark_app:

            - existsSQL
            - sqlData
        """

    @abc.abstractmethod
    def compute_executor_info(
        self, raw_data: SparkApplicationRawDataType, spark_app: SparkApplication
    ) -> SparkApplication:
        """
        This method is responsible for setting the following fields on spark_app:

            - existsExecutors
            - executorData
        """

    @abc.abstractmethod
    def compute_all_job_data(
        self, raw_data: SparkApplicationRawDataType, spark_app: SparkApplication
    ) -> SparkApplication:
        """
        This method is responsible for setting the following fields on spark_app:

            - jobData
        """

    @abc.abstractmethod
    def compute_all_stage_data(
        self, raw_data: SparkApplicationRawDataType, spark_app: SparkApplication
    ) -> SparkApplication:
        """
        This method is responsible for setting the following fields on spark_app:

            - stageData
        """

    @abc.abstractmethod
    def compute_all_task_data(
        self, raw_data: SparkApplicationRawDataType, spark_app: SparkApplication
    ) -> SparkApplication:
        """
        This method is responsible for setting the following fields on spark_app:

            - taskData
        """

    @abc.abstractmethod
    def compute_all_driver_accum_data(
        self, raw_data: SparkApplicationRawDataType, spark_app: SparkApplication
    ) -> SparkApplication:
        """
        This method is responsible for setting the following fields on spark_app:

            - accumData
        """

    @abc.abstractmethod
    def compute_all_metadata(
        self, raw_data: SparkApplicationRawDataType, spark_app: SparkApplication
    ) -> SparkApplication:
        """
        This method is responsible for setting the following fields on spark_app:

            - metadata
        """

    @abc.abstractmethod
    def compute_recent_events(
        self, raw_data: SparkApplicationRawDataType, spark_app: SparkApplication
    ) -> SparkApplication:
        """
        This method is responsible for updating the "time_since_last_event" value on both:

            - spark_app.stageData
            - spark_app.sqlData

        This value should be a list of timestamps where each timestamp is the most recent Task or SQL event
        to complete before this stage/SQL event started executing.
        """

    def construct_spark_application(self, key: SparkApplicationLoaderKey,
                                    raw_data: SparkApplicationRawDataType) -> SparkApplication:
        """
        Generic 'recipe' for constructing a SparkApplication from some raw source of data.
        """
        spark_app = self.init_spark_application(raw_data)
        spark_app = self.compute_sql_info(raw_data, spark_app)
        spark_app = self.compute_executor_info(raw_data, spark_app)
        spark_app = self.compute_all_job_data(raw_data, spark_app)
        spark_app = self.compute_all_task_data(raw_data, spark_app)
        spark_app = self.compute_all_stage_data(raw_data, spark_app)
        spark_app = self.compute_all_driver_accum_data(raw_data, spark_app)
        spark_app = self.compute_all_metadata(raw_data, spark_app)
        spark_app = self.compute_recent_events(raw_data, spark_app)
        return spark_app

    async def batch_load_fn(self, keys: list[SparkApplicationLoaderKey]):
        raw_datas = await self.load_raw_datas(keys)
        return [self.construct_spark_application(*kv) for kv in zip(keys, raw_datas)]


class ParsedLogSparkApplicationLoader(AbstractSparkApplicationDataLoader[str, dict]):
    """
    Creates a SparkApplication from a parsed JSON representation of that application. Useful for re-hydrating
    parsed logs that were saved somewhere (or submitted directly to us)
    """

    _json_data_loader: JSONBlobDataLoader

    def __init__(self, json_loader: JSONBlobDataLoader, **kwargs):
        super().__init__(**kwargs)

        self._json_data_loader = json_loader

    async def load_raw_datas(self, keys) -> list[dict]:
        """
        Loads many already-parsed eventlogs from the provided filepaths
        """
        return await self._json_data_loader.load_many(keys)

    def init_spark_application(self, raw_data) -> SparkApplication:
        return super().init_spark_application(raw_data)

    def compute_recent_events(
        self, raw_data: dict, spark_app: SparkApplication
    ) -> SparkApplication:
        """
        'Recent Events' are really injected into other parts of the SparkApplication during initial computation
        So since we are 'rehydrating' a SparkApplication here, we can assume the recent event data is already
        in the proper places in our raw_data
        """
        return spark_app

    def compute_sql_info(self, raw_data: dict, spark_app: SparkApplication) -> SparkApplication:
        metadata = raw_data.get("metadata", {})
        spark_app.existsSQL = existsSQL = metadata.get("existsSQL", False)
        if existsSQL:
            spark_app.sqlData = pd.DataFrame.from_dict(raw_data["sqlData"]).set_index("sql_id")

        return spark_app

    def compute_executor_info(
        self, raw_data: dict, spark_app: SparkApplication
    ) -> SparkApplication:
        spark_app.existsExecutors = exists_executors = raw_data.get("metadata", {}).get(
            "existsExecutors", False
        )
        if exists_executors:
            spark_app.executorData = pd.DataFrame.from_dict(raw_data["executors"]).set_index(
                "executor_id"
            )

        return spark_app

    def compute_all_job_data(self, raw_data: dict, spark_app: SparkApplication) -> SparkApplication:
        # SPC113 - SDG
        # Because of the way jobData is created, if there are no job Events in the eventlog then the
        # correct fields will not exist. A second condition checking for the 'job_id' field is
        # necessary here to ensure this method will run if this is the case.
        #
        # Note: stageData is initialized differently so this same issue does not exist for that
        # structure. Furthermore, in the event that 'jobData' has no values within, 'stageData' will
        # also have no values and an invalidLog exception will be thrown during log validation
        # in SparkApplicationAdvanced.
        job_data = raw_data.get("jobData", None)
        if job_data is not None and ("job_id" in job_data):
            df = pd.DataFrame.from_dict(raw_data["jobData"])
            df.set_index("job_id")
            spark_app.jobData = df

        return spark_app

    def compute_all_stage_data(
        self, raw_data: dict, spark_app: SparkApplication
    ) -> SparkApplication:
        if "stageData" in raw_data:
            spark_app.stageData = pd.DataFrame.from_dict(raw_data["stageData"]).set_index(
                "stage_id"
            )

        return spark_app

    def compute_all_task_data(
        self, raw_data: dict, spark_app: SparkApplication
    ) -> SparkApplication:
        if "taskData" in raw_data:
            spark_app.taskData = pd.DataFrame.from_dict(raw_data["taskData"]).set_index("task_id")

        return spark_app

    def compute_all_driver_accum_data(
        self, raw_data: dict, spark_app: SparkApplication
    ) -> SparkApplication:
        if "accumData" in raw_data:
            spark_app.accumData = pd.DataFrame.from_dict(raw_data["accumData"])
            if "sql_id" in spark_app.accumData.columns:
                spark_app.accumData = spark_app.accumData.set_index("sql_id")

        return spark_app

    def compute_all_metadata(self, raw_data: dict, spark_app: SparkApplication) -> SparkApplication:
        spark_app.metadata = raw_data.get("metadata", {})
        return spark_app


class UnparsedLogSparkApplicationLoader(AbstractSparkApplicationDataLoader[str, ApplicationModel]):
    """
    From a raw set of Spark log lines, constructs a SparkApplication
    """
    _json_lines_loader: JSONLinesDataLoader

    def __init__(
        self,
        json_lines_loader: JSONLinesDataLoader,
        stdout_path: str = None,
        debug: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.stdout_path = stdout_path
        self.debug = debug

        self._json_lines_loader = json_lines_loader

    @staticmethod
    def validate_app_model(app_model: ApplicationModel):
        match app_model:
            case ApplicationModel(cloud_platform="emr"):
                val1 = ConfigValidationEMR(app=app_model)

            case ApplicationModel(cloud_platform="databricks"):
                val1 = ConfigValidationDatabricks(app=app_model)

            case _:
                raise ValueError(
                    f"Unknown cloud_platform {app_model.cloud_platform} provided in app_model"
                )

        val1.validate()

        val2 = EventDataValidation(app=app_model)
        val2.validate()

    async def load_raw_datas(self, keys: list[str]) -> list[ApplicationModel]:
        """
        Returns a list of ApplicationModels, provided some keys pointing to some raw eventlog file locations. These
        models have not yet been validated, since we are loading the "raw" data here.
        """
        if self._json_lines_loader is None:
            raise RuntimeError("Instance was initialized without a json_lines_loader, and therefore can't be used "
                               + "to load raw data.")

        raw_datas = await self._json_lines_loader.load_many(keys)

        app_models = [ApplicationModel(log_lines=raw_data) for raw_data in raw_datas]

        return app_models

    def init_spark_application(self, raw_data: ApplicationModel) -> SparkApplication:
        self.validate_app_model(raw_data)
        return super().init_spark_application(raw_data)

    def compute_sql_info(
        self, raw_data: ApplicationModel, spark_app: SparkApplication
    ) -> SparkApplication:
        # Get sql info if it exists
        app_model = raw_data
        if not (hasattr(app_model, "sql") and app_model.sql):
            logger.warning("No sql attribute found.")
            spark_app.existsSQL = False
            return spark_app

        spark_app.existsSQL = True
        dfs: list[pd.DataFrame] = []
        for sqlid, sql in app_model.sql.items():
            sql_jobs = []
            sql_stages = []
            sql_tasks = []

            # Sometimes an SQL event will be missing. To be informative, both
            # events must be present. But this information is not critical, so
            # if either event is missing then simply reject the SQL data
            if "start_time" not in sql.keys() or "end_time" not in sql.keys():
                continue

            for jid, job in app_model.jobs.items():
                if (job.submission_time >= sql["start_time"]) and (
                    job.submission_time <= sql["end_time"]
                ):
                    if "completion_time" not in job.__dict__:
                        logger.debug(
                            f"Job {jid} missing completion time. Substituting with associated SQL {sqlid} completion time"
                        )
                        job.completion_time = sql["end_time"]

                    sql_jobs.append(jid)
                    for sid, stage in job.stages.items():
                        sql_stages.append(sid)

                        for task in stage.tasks:
                            sql_tasks.append(task.task_id)
            dfs.append(pd.DataFrame.from_dict({
                "sql_id": [sqlid],
                "description": sql["description"],
                "start_time": [sql["start_time"] - app_model.start_time],
                "end_time": [sql["end_time"] - app_model.start_time],
                "duration": [sql["end_time"] - sql["start_time"]],
                "job_ids": [sql_jobs],
                "stage_ids": [sql_stages],
                "task_ids": [sql_tasks],
            }))

        df = (
            pd.concat(dfs)
            # Remove any rows that have duplicate sql_id column values
            .drop_duplicates(keep="first", subset="sql_id")
            .sort_values(by="sql_id")
            .set_index("sql_id")
        )

        spark_app.sqlData = df
        return spark_app

    def compute_executor_info(
        self, raw_data: ApplicationModel, spark_app: SparkApplication
    ) -> SparkApplication:
        app_model = raw_data
        if not (hasattr(app_model, "executors") and app_model.executors):
            logger.warning("Executor attribute not found.")
            return spark_app

        spark_app.existsExecutors = True
        df = defaultdict(lambda: [])
        for xid, executor in app_model.executors.items():

            # Special case for handling end_time
            if executor.end_time is not None:
                end_time = executor.end_time / 1000 - app_model.start_time
            else:
                end_time = executor.end_time

            df["executor_id"].append(xid)
            df["cores"].append(executor.cores)
            df["start_time"].append(executor.start_time / 1000 - app_model.start_time)
            df["end_time"].append(end_time)
            df["host"].append(executor.host)
            df["removed_reason"].append(executor.removed_reason)

        df = pd.DataFrame(df)
        df = df.sort_values(["executor_id"])
        df = df.set_index("executor_id")
        spark_app.executorData = df

        return spark_app

    def compute_all_job_data(
        self, raw_data: ApplicationModel, spark_app: SparkApplication
    ) -> SparkApplication:
        app_model = raw_data
        t1 = time.time()
        dfs: list[pd.DataFrame] = []
        ref_time = app_model.start_time
        for jid, job in app_model.jobs.items():

            stage_ids = []
            for sid, stage in job.stages.items():
                stage_ids.append(sid)

            dfs.append(pd.DataFrame.from_dict(
                {
                    "job_id": [jid],
                    "sql_id": None,
                    "stage_ids": [stage_ids],
                    "submission_time": [job.submission_time - ref_time],
                    "completion_time": [job.completion_time - ref_time],
                    "duration": [job.completion_time - job.submission_time],
                    "submission_timestamp": [job.submission_time],
                    "completion_timestamp": [job.completion_time],
                }
            ))

        df = pd.concat(dfs)
        if len(df) > 0:
            df = df.sort_values(by="job_id")
            df = df.set_index("job_id")

            # Get the query-id for each job if it exists
            if spark_app.existsSQL:
                for qid, row in spark_app.sqlData.iterrows():
                    for jid in row["job_ids"]:
                        df.at[jid, "sql_id"] = qid

        logger.info("Aggregated job data [%.2f]" % (time.time() - t1))

        spark_app.jobData = df

        return spark_app

    def compute_all_task_data(
        self, raw_data: ApplicationModel, spark_app: SparkApplication
    ) -> SparkApplication:
        app_model = raw_data
        # Time task data extraction
        t1 = time.time()
        ref_time = app_model.start_time

        # Extract task IDs from within queries
        tid2qid = defaultdict(lambda: [])
        if spark_app.existsSQL:
            for qid, query in spark_app.sqlData.iterrows():
                for tid in query.task_ids:
                    tid2qid[tid] = qid

        # Basic task performance metrics
        task_id = []
        sql_id = []
        job_id = []
        exec_id = []
        killed = []
        speculative = []
        start_time = []
        end_time = []
        duration = []
        # input_mb       = []
        remote_mb_read = []
        locality = []

        # Disk-based performance metrics
        input_mb = []
        output_mb = []
        peak_execution_memory = []
        shuffle_mb_written = []
        remote_mb_read = []
        memory_bytes_spilled = []
        disk_bytes_spilled = []
        result_size = []

        # Time-based performance metrics
        executor_run_time = []
        executor_deserialize_time = []
        executor_cpu_time = []
        result_serialization_time = []
        gc_time = []
        scheduler_delay = []
        fetch_wait_time = []
        shuffle_write_time = []
        local_read_time = []
        compute_time = []
        task_compute_time = []
        input_read_time = []
        output_write_time = []

        # Memory usage metrics
        jvm_virtual_memory = []
        jvm_rss_memory = []
        python_virtual_memory = []
        python_rss_memory = []
        other_virtual_memory = []
        other_rss_memory = []

        # Parse through job, stage-level metrics and extract task-level data
        stage_id = []
        for jid, job in app_model.jobs.items():
            for sid, stage in job.stages.items():
                for task in stage.tasks:
                    # Basic task performance metrics
                    sql_id.append(tid2qid[task.task_id])
                    stage_id.append(sid)
                    job_id.append(jid)
                    task_id.append(task.task_id)
                    exec_id.append(task.executor_id)
                    killed.append(task.killed)
                    speculative.append(task.speculative)
                    start_time.append(task.start_time - ref_time)
                    end_time.append(task.finish_time - ref_time)
                    duration.append(task.finish_time - task.start_time)

                    # Disk-based performance metrics
                    input_mb.append(task.input_mb)
                    output_mb.append(task.output_mb)
                    peak_execution_memory.append(task.peak_execution_memory)
                    shuffle_mb_written.append(task.shuffle_mb_written)
                    remote_mb_read.append(task.remote_mb_read)
                    memory_bytes_spilled.append(task.memory_bytes_spilled)
                    disk_bytes_spilled.append(task.disk_bytes_spilled)
                    result_size.append(task.result_size)
                    locality.append(task.locality)

                    # Time-based performance metrics
                    executor_run_time.append(task.executor_run_time)
                    executor_deserialize_time.append(task.executor_deserialize_time)
                    result_serialization_time.append(task.result_serialization_time)
                    executor_cpu_time.append(task.executor_cpu_time)
                    gc_time.append(task.gc_time)
                    scheduler_delay.append(task.scheduler_delay)
                    fetch_wait_time.append(task.fetch_wait)
                    shuffle_write_time.append(task.shuffle_write_time)
                    local_read_time.append(task.local_read_time)
                    compute_time.append(task.compute_time_without_gc())
                    task_compute_time.append(task.task_compute_time())
                    input_read_time.append(task.input_read_time)
                    output_write_time.append(task.output_write_time)

                    # Memory usage metrics
                    jvm_virtual_memory.append(task.jvm_v_memory)
                    jvm_rss_memory.append(task.jvm_rss_memory)
                    python_virtual_memory.append(task.python_v_memory)
                    python_rss_memory.append(task.python_rss_memory)
                    other_virtual_memory.append(task.other_v_memory)
                    other_rss_memory.append(task.other_rss_memory)

        # Pack all task-level data into a Pandas dataframe
        raw_frame = {
            # Basic task performance metrics
            "task_id": task_id,
            "sql_id": sql_id,
            "job_id": job_id,
            "stage_id": stage_id,
            "executor_id": exec_id,
            "killed": killed,
            "speculative": speculative,
            "start_time": start_time,
            "end_time": end_time,
            "duration": duration,
            "locality": locality,
            # Disk-based performance metrics
            "input_mb": input_mb,
            "output_mb": output_mb,
            "peak_execution_memory": peak_execution_memory,
            "shuffle_mb_written": shuffle_mb_written,
            "remote_mb_read": remote_mb_read,
            "memory_bytes_spilled": memory_bytes_spilled,
            "disk_bytes_spilled": disk_bytes_spilled,
            "result_size": result_size,
            # Time-based performance metrics
            "executor_run_time": executor_run_time,
            "executor_deserialize_time": executor_deserialize_time,
            "result_serialization_time": result_serialization_time,
            "executor_cpu_time": executor_cpu_time,
            "gc_time": gc_time,
            "scheduler_delay": scheduler_delay,
            "fetch_wait_time": fetch_wait_time,
            "shuffle_write_time": shuffle_write_time,
            "local_read_time": local_read_time,
            "compute_time": compute_time,
            "task_compute_time": task_compute_time,
            "input_read_time": input_read_time,
            "output_write_time": output_write_time,
            # Memory usage metrics
            "jvm_virtual_memory": jvm_virtual_memory,
            "jvm_rss_memory": jvm_rss_memory,
            "python_virtual_memory": python_virtual_memory,
            "python_rss_memory": python_rss_memory,
            "other_virtual_memory": other_virtual_memory,
            "other_rss_memory": other_rss_memory,
        }

        df = (
            pd.DataFrame(raw_frame)
            # Remove any rows that have duplicate task_id column values
            .drop_duplicates(keep="first", subset="task_id")
            .sort_values(by="task_id")
            .set_index("task_id")
        )

        # Report timing and save the dataframe
        logger.info("Aggregated task data [%.2fs]" % (time.time() - t1))
        spark_app.taskData = df
        return spark_app

    def compute_all_stage_data(
        self, raw_data: ApplicationModel, spark_app: SparkApplication
    ) -> SparkApplication:
        app_model = raw_data
        t1 = time.time()

        sid2qid = defaultdict(lambda: [])
        if spark_app.existsSQL:
            for qid, query in spark_app.sqlData.iterrows():
                for sid in query.stage_ids:
                    sid2qid[sid] = qid

        stage_id = []
        query_id = []
        job_id = []
        start_time = []
        end_time = []
        duration = []
        num_tasks = []
        task_time = []

        input_mb = []
        output_mb = []
        peak_execution_memory = []
        shuffle_mb_written = []
        remote_mb_read = []
        memory_bytes_spilled = []
        disk_bytes_spilled = []
        result_size = []

        executor_run_time = []
        executor_deserialize_time = []
        result_serialization_time = []
        executor_cpu_time = []
        gc_time = []
        scheduler_delay = []
        fetch_wait_time = []
        local_read_time = []
        compute_time = []
        task_compute_time = []
        input_read_time = []
        output_write_time = []
        shuffle_write_time = []

        task_ids = []
        parents = []
        rdd_ids = []
        stage_info = []

        for jid, job in app_model.jobs.items():
            for sid, stage in job.stages.items():

                # Get the task-ids for this stage
                taskids = []
                for task in stage.tasks:
                    taskids.append(task.task_id)

                # Get the task data for this stage
                task_data = spark_app.taskData.loc[taskids]

                stage_id.append(sid)
                query_id.append(sid2qid[sid])
                job_id.append(jid)
                task_ids.append(taskids)
                parents.append(app_model.dag.parents_dag_dict[sid])
                rdd_ids.append(app_model.dag.stage_rdd_dict[sid])

                stage_info.append(
                    {
                        "stage_name": stage.stage_name,
                        "num_tasks": stage.num_tasks,
                        "num_rdds": len(stage.stage_info["RDD Info"]),
                        "num_parents": len(stage.stage_info["Parent IDs"]),
                        "final_rdd_name": stage.stage_info["RDD Info"][0]["Name"],
                    }
                )

                start_time.append(task_data["start_time"].min())
                end_time.append(task_data["end_time"].max())
                duration.append(task_data["end_time"].max() - task_data["start_time"].min())
                num_tasks.append(len(task_data.index))
                task_time.append(task_data["duration"].sum())

                input_mb.append(task_data["input_mb"].sum())
                output_mb.append(task_data["output_mb"].sum())
                peak_execution_memory.append(task_data["peak_execution_memory"].max())
                shuffle_mb_written.append(task_data["shuffle_mb_written"].sum())
                remote_mb_read.append(task_data["remote_mb_read"].sum())
                memory_bytes_spilled.append(task_data["memory_bytes_spilled"].sum())
                disk_bytes_spilled.append(task_data["disk_bytes_spilled"].sum())
                result_size.append(task_data["result_size"].sum())

                executor_run_time.append(task_data["executor_run_time"].sum())
                executor_deserialize_time.append(task_data["executor_deserialize_time"].sum())
                result_serialization_time.append(task_data["result_serialization_time"].sum())
                executor_cpu_time.append(task_data["executor_cpu_time"].sum())
                gc_time.append(task_data["gc_time"].sum())
                scheduler_delay.append(task_data["scheduler_delay"].sum())
                fetch_wait_time.append(task_data["fetch_wait_time"].sum())
                local_read_time.append(task_data["local_read_time"].sum())
                compute_time.append(task_data["compute_time"].sum())
                task_compute_time.append(task_data["task_compute_time"].sum())
                input_read_time.append(task_data["input_read_time"].sum())
                output_write_time.append(task_data["output_write_time"].sum())
                shuffle_write_time.append(task_data["shuffle_write_time"].sum())

        raw_frame = {
            "stage_id": stage_id,
            "query_id": query_id,
            "job_id": job_id,
            "task_ids": task_ids,
            "parents": parents,
            "rdd_ids": rdd_ids,
            "stage_info": stage_info,
            "start_time": start_time,
            "end_time": end_time,
            "duration": duration,
            "num_tasks": num_tasks,
            "task_time": task_time,
            "input_mb": input_mb,
            "output_mb": output_mb,
            "peak_execution_memory": peak_execution_memory,
            "shuffle_mb_written": shuffle_mb_written,
            "remote_mb_read": remote_mb_read,
            "memory_bytes_spilled": memory_bytes_spilled,
            "disk_bytes_spilled": disk_bytes_spilled,
            "result_size": result_size,
            "executor_run_time": executor_run_time,
            "executor_deserialize_time": executor_deserialize_time,
            "result_serialization_time": result_serialization_time,
            "executor_cpu_time": executor_cpu_time,
            "gc_time": gc_time,
            "scheduler_delay": scheduler_delay,
            "fetch_wait_time": fetch_wait_time,
            "local_read_time": local_read_time,
            "compute_time": compute_time,
            "task_compute_time": task_compute_time,
            "input_read_time": input_read_time,
            "output_write_time": output_write_time,
            "shuffle_write_time": shuffle_write_time,
        }
        df = (
            pd.DataFrame(raw_frame)
            # Remove any rows that have duplicate stage_id column values
            .drop_duplicates(keep="first", subset="stage_id")
            .sort_values(by="stage_id")
            .set_index("stage_id")
        )

        logger.info("Aggregated stage data [%.2fs]" % (time.time() - t1))
        spark_app.stageData = df
        return spark_app

    def compute_all_driver_accum_data(
        self, raw_data: ApplicationModel, spark_app: SparkApplication
    ) -> SparkApplication:
        app_model = raw_data
        t1 = time.time()

        df = (
            pd.DataFrame(app_model.accum_metrics)
            .transpose()
            # only driver accum values are updated
            .dropna()
        )

        if "value" in df.columns:
            # get start and end times of sql_id
            if spark_app.existsSQL:
                start_times = []
                end_times = []
                for index, row in df.iterrows():
                    sql_row = spark_app.sqlData.loc[row["sql_id"]]
                    start_times.append(sql_row.at["start_time"])
                    end_times.append(sql_row.at["end_time"])

                df["start_times"] = start_times
                df["end_times"] = end_times
        # if not driver accum update values, then empty dataframe
        else:
            df = pd.DataFrame()
        logger.info("Aggregated accum data [%.2fs]" % (time.time() - t1))

        spark_app.accumData = df
        return spark_app

    def compute_all_metadata(
        self, raw_data: ApplicationModel, spark_app: SparkApplication
    ) -> SparkApplication:
        app_model = raw_data
        spark_app.metadata = {
            "application_info": {
                "timestamp_start_ms": int(app_model.start_time * 1000),
                "timestamp_end_ms": int(app_model.finish_time * 1000),
                "runtime_sec": app_model.finish_time - app_model.start_time,
                "name": app_model.app_name,
                "id": app_model.spark_metadata["spark.app.id"],
                "spark_version": app_model.spark_version,
                "emr_version_tag": app_model.emr_version_tag,
                "cloud_platform": app_model.cloud_platform,
                "cloud_provider": app_model.cloud_provider,
                "cluster_id": app_model.cluster_id,
            },
            "spark_params": app_model.spark_metadata,
            "existsSQL": spark_app.existsSQL,
            "existsExecutors": spark_app.existsExecutors,
        }
        return spark_app

    def compute_recent_events(
        self, raw_data: ApplicationModel, spark_app: SparkApplication
    ) -> SparkApplication:
        tcomp = np.concatenate(([0.0], spark_app.taskData["end_time"].values))

        if spark_app.existsSQL:
            tcomp = np.concatenate(
                (
                    tcomp,
                    spark_app.sqlData["start_time"].values,
                    spark_app.sqlData["end_time"].values,
                )
            )

        trecent = []
        for sid in spark_app.stageData.index.values:
            tstart = spark_app.stageData.loc[sid]["start_time"]
            trecent.append(tstart - tcomp[tcomp < tstart].max())

        spark_app.stageData["time_since_last_event"] = trecent

        if spark_app.existsSQL:
            trecent = []
            for qid in spark_app.sqlData.index.values:
                tstart = spark_app.sqlData.loc[qid]["start_time"]

                tmp = tcomp[tcomp < tstart]

                if len(tmp) == 0:
                    trecent.append(0)
                else:
                    trecent.append(tstart - tcomp[tcomp < tstart].max())
            spark_app.sqlData["time_since_last_event"] = trecent

        return spark_app


class AbstractAmbiguousLogFormatSparkApplicationLoader(
        AbstractSparkApplicationDataLoader[SparkApplicationLoaderKey, tuple[bool, dict | ApplicationModel]], abc.ABC):

    _parsed_app_loader: ParsedLogSparkApplicationLoader
    _unparsed_app_loader: UnparsedLogSparkApplicationLoader
    _json_lines_loader: JSONLinesDataLoader

    def __init__(
        self,
        json_lines_loader: JSONLinesDataLoader,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self._json_lines_loader = json_lines_loader
        # These "sub-loaders" won't actually be loading the raw data, so we don't need to pass them any dataloaders
        #  We just want to use them to construct our SparkApplications based on whether the data handed to us is a
        #  parsed or unparsed eventlog
        self._parsed_app_loader = ParsedLogSparkApplicationLoader(None)
        self._unparsed_app_loader = UnparsedLogSparkApplicationLoader(None)

    def _construct_from_parsed_representation(self, key: str, data: tuple[bool, dict]):
        return self._parsed_app_loader.construct_spark_application(key, data)

    def _construct_from_unparsed_representation(self, key: str, data: tuple[bool, ApplicationModel]):
        return self._unparsed_app_loader.construct_spark_application(key, data)

    async def _load_raw_datas(self, keys: list[str]) -> list[tuple[bool, dict | ApplicationModel]]:
        """
        Given some eventlog locations, determines the data format of the file (i.e. raw vs already-parsed) and returns
        the appropriate in-memory representation of that file.
        """
        raw_datas = await self._json_lines_loader.load_many(keys)

        ret = []
        for key, raw_data in zip(keys, raw_datas):
            line = next(raw_data)
            # This assumes that for parsed apps, the first "line" from the file will be the fully-formed dictionary
            #  representation of a SparkApplication. This may not be true over time... we should strive to keep this
            #  check "cheap", however
            if SparkApplication.is_parsed_spark_app(line):
                ret.append((True, line))
            else:
                # ApplicationModel expects to receive all the lines, so just wrap the line we already read in a
                #  generator so that we can re-yield it appropriately
                def lines():
                    yield line
                    yield from raw_data

                try:
                    app_model = ApplicationModel(log_lines=lines())
                    ret.append((False, app_model))
                except Exception as e:
                    logger.error(f"Encountered an exception loading eventlog located at: {key}", exc_info=e)
                    ret.append((False, None))

        return ret

    def _construct_base_spark_application(self, key: str,
                                          raw_data: tuple[bool, dict | ApplicationModel]) -> SparkApplication:
        """
        Given an initial piece of raw_data, calls into the appropriate "sub-loader" based on the detected file format,
        i.e. whether the eventlog was delivered to us already-parsed.
        """
        is_parsed, data = raw_data

        # If we weren't able to create a SparkApplication out of one of the "keys" provided to us, we still need to
        #  return something in order to adhere to the DataLoader batch API
        if data is None:
            spark_app = None
        elif is_parsed:
            spark_app = self._construct_from_parsed_representation(key, data)
        else:
            spark_app = self._construct_from_unparsed_representation(key, data)

        return spark_app

    # None of these abstract methods actually need to be implemented because we will be calling into the proper
    #  un/parsed SparkApplication loader based on the underlying data, and those loaders have these methods
    #  implemented already
    def init_spark_application(self, raw_data) -> SparkApplication:
        """See comment above for why this is left un-implemented"""
        pass

    def compute_sql_info(self, raw_data: SparkApplicationRawDataType, spark_app: SparkApplication) -> SparkApplication:
        """See comment above for why this is left un-implemented"""
        pass

    def compute_executor_info(self, raw_data: SparkApplicationRawDataType,
                              spark_app: SparkApplication) -> SparkApplication:
        """See comment above for why this is left un-implemented"""
        pass

    def compute_all_job_data(self, raw_data: SparkApplicationRawDataType,
                             spark_app: SparkApplication) -> SparkApplication:
        """See comment above for why this is left un-implemented"""
        pass

    def compute_all_task_data(self, raw_data: SparkApplicationRawDataType,
                              spark_app: SparkApplication) -> SparkApplication:
        """See comment above for why this is left un-implemented"""
        pass

    def compute_all_stage_data(self, raw_data: SparkApplicationRawDataType,
                               spark_app: SparkApplication) -> SparkApplication:
        """See comment above for why this is left un-implemented"""
        pass

    def compute_all_driver_accum_data(self, raw_data: SparkApplicationRawDataType,
                                      spark_app: SparkApplication) -> SparkApplication:
        """See comment above for why this is left un-implemented"""
        pass

    def compute_all_metadata(self, raw_data: SparkApplicationRawDataType,
                             spark_app: SparkApplication) -> SparkApplication:
        """See comment above for why this is left un-implemented"""
        pass

    def compute_recent_events(self, raw_data: SparkApplicationRawDataType,
                              spark_app: SparkApplication) -> SparkApplication:
        """See comment above for why this is left un-implemented"""
        pass


class AmbiguousLogFormatSparkApplicationLoader(AbstractAmbiguousLogFormatSparkApplicationLoader[str]):
    """
    Much of the time, we may not know whether a file given to us is for a parsed or unparsed eventlog without opening it
    up first. But, we don't want to have to open up a file and just throw it away if it's not what we initially
    expected. This class, then, may be used when this information is ambiguous to us, and it will handle calling into
    the proper "sub-loader" transparently (and without having to re-load anything)
    """

    def _construct_from_parsed_representation(self, key: str, data: tuple[bool, dict]):
        return self._parsed_app_loader.construct_spark_application(key, data)

    def _construct_from_unparsed_representation(self, key: str, data: tuple[bool, ApplicationModel]):
        return self._unparsed_app_loader.construct_spark_application(key, data)

    async def _load_raw_datas(self, keys: list[str]) -> list[tuple[bool, dict | ApplicationModel]]:
        """

        """
        raw_datas = await self._json_lines_loader.load_many(keys)

        ret = []
        for raw_data in raw_datas:
            line = next(raw_data)
            # This assumes that for parsed apps, the first "line" from the file will be the fully-formed dictionary
            #  representation of a SparkApplication. This may not be true over time...
            if SparkApplication.is_parsed_spark_app(line):
                ret.append((True, line))
            else:
                # ApplicationModel expects to receive all the lines, so just wrap the line we already read in a
                #  generator so that we can re-yield it appropriately
                def lines():
                    yield line
                    yield from raw_data

                app_model = ApplicationModel(log_lines=lines())
                try:
                    UnparsedLogSparkApplicationLoader.validate_app_model(app_model)
                    ret.append((False, app_model))
                except SyncParserException as e:
                    logger.warning(e)
                    ret.append((False, None))

        return ret

    def _construct_base_spark_application(self, key: str,
                                          raw_data: tuple[bool, dict | ApplicationModel]) -> SparkApplication:
        """

        """
        is_parsed, data = raw_data

        # If we weren't able to create a SparkApplication out of one of the "keys" provided to us, we still need to
        #  return something in order to adhere to the DataLoader batch API
        if data is None:
            spark_app = None
        elif is_parsed:
            spark_app = self._construct_from_parsed_representation(key, data)
        else:
            spark_app = self._construct_from_unparsed_representation(key, data)

        return spark_app

    async def load_raw_datas(self, keys: list[str]) -> list[tuple[bool, dict | ApplicationModel]]:
        return await self._load_raw_datas(keys)

    def construct_spark_application(self, key: str, raw_data: tuple[bool, dict | ApplicationModel]) -> SparkApplication:
        return self._construct_base_spark_application(key, raw_data)


def create_spark_application(*, path, thresholds=None) -> SparkApplication:
    """
    Convenience function for constructing SparkApplication objects
    """
    if not path:
        raise ValueError("No provided eventlog location.")

    path = str(path)
    parsed_path = urlparse(path)

    thresholds = thresholds if thresholds is not None else ArchiveExtractionThresholds()

    async def create_spark_app():
        match parsed_path.scheme:
            case "s3":
                file_loader = S3FileLinesDataLoader(extraction_thresholds=thresholds)

            case "http" | "https":
                file_loader = HTTPFileLinesDataLoader(extraction_thresholds=thresholds)

            case "file" | _:
                file_loader = LocalFileLinesDataLoader(extraction_thresholds=thresholds)

        json_loader = JSONLinesDataLoader(lines_data_loader=file_loader)
        app_loader = AmbiguousLogFormatSparkApplicationLoader(json_lines_loader=json_loader)

        return await app_loader.load(path)

    return asyncio.run(create_spark_app())

import gzip
import json
import logging
import os
import time
from collections import defaultdict

import boto3
import numpy as np
import pandas as pd

from .application_model import ApplicationModel
from .validation_configs import ConfigValidationDatabricks, ConfigValidationEMR
from .validation_event_data import EventDataValidation


class sparkApplication:
    def __init__(
        self,
        spark_eventlog_parsed_path=None,
        spark_eventlog_path=None,
        stdout=None,
        debug=False,
    ):

        self.spark_eventlog_path = spark_eventlog_path
        self.spark_eventlog_parsed_path = spark_eventlog_parsed_path
        self.existsSQL = False
        self.existsExecutors = False
        self.metadata = {}
        self.stdout = stdout
        self.debug = debug

        if self.spark_eventlog_parsed_path is not None:
            self.load(filepath=self.spark_eventlog_parsed_path)

        elif self.spark_eventlog_path is not None:

            t0 = time.time()
            if "s3://" in self.spark_eventlog_path:
                path = self.spark_eventlog_path.replace("s3://", "").split("/")
                bucket = path[0]
                path = "/".join(path[1:])
            else:
                path = self.spark_eventlog_path
                bucket = None

            appobj = ApplicationModel(
                eventlogpath=path, bucket=bucket, stdoutpath=stdout, debug=debug
            )
            logging.info("Loaded object from spark eventlog [%.2fs]" % (time.time() - t0))

            self.validate_app(appobj, self.debug)

            # Get sql info if it exists
            if hasattr(appobj, "sql") and appobj.sql:
                self.getSQLinfo(appobj)
                self.existsSQL = True
            else:
                logging.warning("No sql attribute found.")

            if hasattr(appobj, "executors") and appobj.executors:
                self.getExecutorInfo(appobj)
                self.existsExecutors = True
            else:
                logging.warning("Executor attribute not found.")

            self.getAllJobData(appobj)
            self.getAllTaskData(appobj)
            self.getAllStageData(appobj)

            # Remove duplicated rows
            if self.existsSQL:
                self.sqlData = self.sqlData[~self.sqlData.index.duplicated(keep="first")]
            self.stageData = self.stageData[~self.stageData.index.duplicated(keep="first")]
            self.taskData = self.taskData[~self.taskData.index.duplicated(keep="first")]

            self.getAllDriverAccumData(appobj)
            self.getAllMetaData(appobj)
            self.getRecentEvents()
            # self.crossReferenceData(appobj)
            logging.info("sparkApplication object creation complete")

    def validate_app(self, appobj, debug):
        if appobj.cloud_platform == "emr":
            val1 = ConfigValidationEMR(app=appobj, debug=debug)
        elif appobj.cloud_platform == "databricks":
            val1 = ConfigValidationDatabricks(app=appobj, debug=debug)

        val1.validate()

        val2 = EventDataValidation(app=appobj, debug=debug)
        val2.validate()

    def getRecentEvents(self):

        tcomp = np.concatenate(([0.0], self.taskData["end_time"].values))

        if self.existsSQL:
            tcomp = np.concatenate(
                (tcomp, self.sqlData["start_time"].values, self.sqlData["end_time"].values)
            )

        trecent = []
        for sid in self.stageData.index.values:
            tstart = self.stageData.loc[sid]["start_time"]
            trecent.append(tstart - tcomp[tcomp < tstart].max())

        self.stageData["time_since_last_event"] = trecent

        if self.existsSQL:
            trecent = []
            for qid in self.sqlData.index.values:
                tstart = self.sqlData.loc[qid]["start_time"]

                tmp = tcomp[tcomp < tstart]

                if len(tmp) == 0:
                    trecent.append(0)
                else:
                    trecent.append(tstart - tcomp[tcomp < tstart].max())
            self.sqlData["time_since_last_event"] = trecent

    # This method collects all of the sql information into a dataframe
    def getSQLinfo(self, appobj):

        df = pd.DataFrame([])
        for sqlid, sql in appobj.sql.items():
            sql_jobs = []
            sql_stages = []
            sql_tasks = []

            # Sometimes an SQL event will be missing. To be informative, both
            # events must be present. But this information is not critical, so
            # if either event is missing then simply reject the SQL data
            if "start_time" not in sql.keys() or "end_time" not in sql.keys():
                continue

            for jid, job in appobj.jobs.items():

                if (job.submission_time >= sql["start_time"]) and (
                    job.submission_time <= sql["end_time"]
                ):

                    if "completion_time" not in job.__dict__:
                        logging.debug(
                            f"Job {jid} missing completion time. Substituting with associated SQL {sqlid} completion time"
                        )
                        job.completion_time = sql["end_time"]

                    sql_jobs.append(jid)
                    for sid, stage in job.stages.items():
                        sql_stages.append(sid)

                        for task in stage.tasks:
                            sql_tasks.append(task.task_id)

            df = df.append(
                pd.DataFrame.from_dict(
                    {
                        "sql_id": [sqlid],
                        "description": sql["description"],
                        "start_time": [sql["start_time"] - appobj.start_time],
                        "end_time": [sql["end_time"] - appobj.start_time],
                        "duration": [sql["end_time"] - sql["start_time"]],
                        "job_ids": [sql_jobs],
                        "stage_ids": [sql_stages],
                        "task_ids": [sql_tasks],
                    }
                )
            )

        df = df.sort_values(by="sql_id")
        df = df.set_index("sql_id")
        self.sqlData = df

    def getExecutorInfo(self, appobj):
        df = defaultdict(lambda: [])
        for xid, executor in appobj.executors.items():

            # print(executor.end_time)
            # Special case for handling end_time
            if executor.end_time is not None:
                end_time = executor.end_time / 1000 - appobj.start_time
            else:
                # print('None detected')
                end_time = executor.end_time

            df["executor_id"].append(xid)
            df["cores"].append(executor.cores)
            df["start_time"].append(executor.start_time / 1000 - appobj.start_time)
            df["end_time"].append(end_time)
            df["host"].append(executor.host)
            df["removed_reason"].append(executor.removed_reason)

        df = pd.DataFrame(df)
        df = df.sort_values(["executor_id"])
        df = df.set_index("executor_id")
        self.executorData = df

    # This method collects all of the job-level data for this application
    # into a dataframe
    def getAllJobData(self, appobj):
        t1 = time.time()
        df = pd.DataFrame([])
        refTime = appobj.start_time
        for jid, job in appobj.jobs.items():

            stage_ids = []
            for sid, stage in job.stages.items():
                stage_ids.append(sid)

            df = df.append(
                pd.DataFrame.from_dict(
                    {
                        "job_id": [jid],
                        "sql_id": None,
                        "stage_ids": [stage_ids],
                        "submission_time": [job.submission_time - refTime],
                        "completion_time": [job.completion_time - refTime],
                        "duration": [job.completion_time - job.submission_time],
                        "submission_timestamp": [job.submission_time],
                        "completion_timestamp": [job.completion_time],
                    }
                )
            )

        if len(df) > 0:
            df = df.sort_values(by="job_id")
            df = df.set_index("job_id")

            # Get the query-id for each job if it exists
            if self.existsSQL:
                for qid, row in self.sqlData.iterrows():
                    for jid in row["job_ids"]:
                        df.at[jid, "sql_id"] = qid

        logging.info("Aggregated job data [%.2f]" % (time.time() - t1))

        self.jobData = df

    # This method collects all of the task data for this application into
    # a dataframe
    def getAllTaskData(self, appobj):
        # Time task data extraction
        t1 = time.time()
        refTime = appobj.start_time

        # Extract task IDs from within queries
        tid2qid = defaultdict(lambda: [])
        if self.existsSQL:
            for qid, query in self.sqlData.iterrows():
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
        for jid, job in appobj.jobs.items():
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
                    start_time.append(task.start_time - refTime)
                    end_time.append(task.finish_time - refTime)
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

        df = pd.DataFrame(
            {
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
        )

        df = df.sort_values(by="task_id")
        df = df.set_index("task_id")

        # Report timing and save the dataframe
        logging.info("Aggregated task data [%.2fs]" % (time.time() - t1))
        self.taskData = df

    def getAllStageData(self, appobj):
        t1 = time.time()
        df = pd.DataFrame([])

        sid2qid = defaultdict(lambda: [])
        if self.existsSQL:
            for qid, query in self.sqlData.iterrows():
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

        for jid, job in appobj.jobs.items():
            for sid, stage in job.stages.items():

                # Get the task-ids for this stage
                taskids = []
                for task in stage.tasks:
                    taskids.append(task.task_id)

                # Get the task data for this stage
                taskData = self.taskData.loc[taskids]

                stage_id.append(sid)
                query_id.append(sid2qid[sid])
                job_id.append(jid)
                task_ids.append(taskids)
                parents.append(appobj.dag.parents_dag_dict[sid])
                rdd_ids.append(appobj.dag.stage_rdd_dict[sid])

                stage_info_dict = {
                    "stage_name": stage.stage_name,
                    "num_tasks": stage.num_tasks,
                    "num_rdds": len(stage.stage_info["RDD Info"]),
                    "num_parents": len(stage.stage_info["Parent IDs"]),
                    "final_rdd_name": stage.stage_info["RDD Info"][0]["Name"],
                }
                stage_info.append(stage_info_dict)

                start_time.append(taskData["start_time"].min())
                end_time.append(taskData["end_time"].max())
                duration.append(taskData["end_time"].max() - taskData["start_time"].min())
                num_tasks.append(len(taskData.index))
                task_time.append(taskData["duration"].sum())

                input_mb.append(taskData["input_mb"].sum())
                output_mb.append(taskData["output_mb"].sum())
                peak_execution_memory.append(taskData["peak_execution_memory"].max())
                shuffle_mb_written.append(taskData["shuffle_mb_written"].sum())
                remote_mb_read.append(taskData["remote_mb_read"].sum())
                memory_bytes_spilled.append(taskData["memory_bytes_spilled"].sum())
                disk_bytes_spilled.append(taskData["disk_bytes_spilled"].sum())
                result_size.append(taskData["result_size"].sum())

                executor_run_time.append(taskData["executor_run_time"].sum())
                executor_deserialize_time.append(taskData["executor_deserialize_time"].sum())
                result_serialization_time.append(taskData["result_serialization_time"].sum())
                executor_cpu_time.append(taskData["executor_cpu_time"].sum())
                gc_time.append(taskData["gc_time"].sum())
                scheduler_delay.append(taskData["scheduler_delay"].sum())
                fetch_wait_time.append(taskData["fetch_wait_time"].sum())
                local_read_time.append(taskData["local_read_time"].sum())
                compute_time.append(taskData["compute_time"].sum())
                task_compute_time.append(taskData["task_compute_time"].sum())
                input_read_time.append(taskData["input_read_time"].sum())
                output_write_time.append(taskData["output_write_time"].sum())
                shuffle_write_time.append(taskData["shuffle_write_time"].sum())

        df = pd.DataFrame(
            {
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
        )

        logging.info("Aggregated stage data [%.2fs]" % (time.time() - t1))
        df = df.sort_values(by="stage_id")
        df = df.set_index("stage_id")
        self.stageData = df

    # This method collects all of the driver accumulated metrics for this
    # application into a dataframe
    def getAllDriverAccumData(self, appobj):
        t1 = time.time()

        df = pd.DataFrame(appobj.accum_metrics).T

        # only driver accum values are updated
        df = df.dropna()

        if "value" in df.columns:
            # get start and end times of sql_id
            if self.existsSQL:
                start_times = []
                end_times = []
                for index, row in df.iterrows():

                    start_times.append(self.sqlData.loc[row["sql_id"]].at["start_time"])
                    end_times.append(self.sqlData.loc[row["sql_id"]].at["end_time"])

                df["start_times"] = start_times
                df["end_times"] = end_times
        # if not driver accum update values, then empty dataframe
        else:
            df = pd.DataFrame()
        logging.info("Aggregated accum data [%.2fs]" % (time.time() - t1))

        self.accumData = df

    def getAllMetaData(self, appobj):

        # self.sparkMetadata = (appobj.spark_metadata)

        self.metadata = {
            "application_info": {
                "timestamp_start_ms": int(appobj.start_time * 1000),
                "timestamp_end_ms": int(appobj.finish_time * 1000),
                "runtime_sec": appobj.finish_time - appobj.start_time,
                "name": appobj.app_name,
                "id": appobj.spark_metadata["spark.app.id"],
                "spark_version": appobj.spark_version,
                "emr_version_tag": appobj.emr_version_tag,
                "cloud_platform": appobj.cloud_platform,
                "cloud_provider": appobj.cloud_provider,
                "cluster_id": appobj.cluster_id,
            },
            "spark_params": appobj.spark_metadata,
            "existsSQL": self.existsSQL,
            "existsExecutors": self.existsExecutors,
        }

    def addMetadata(self, key=None, value=None):

        if (key is None) or (value is None):
            logging.error("key and value must both be supplied.")

        self.metadata[key] = value

    def getQueryData(self, sql_id=None):

        sqlData = self.sqlData.loc[sql_id]
        stageData = self.stageData.loc[self.sqlData.loc[sql_id].stage_ids]
        taskData = pd.DataFrame([])
        for sid, stage in stageData.iterrows():
            taskData = taskData.append(self.taskData[self.taskData["stage_id"] == sid])

        return sqlData, taskData, stageData

    def save(self, filepath=None, compress=False):

        # Convert all dataframes into json and aggregate
        # into a single dict
        saveDat = {}
        if hasattr(self, "jobData"):
            saveDat["jobData"] = self.jobData.reset_index().to_dict("list")
        if hasattr(self, "stageData"):
            saveDat["stageData"] = self.stageData.reset_index().to_dict("list")
        if hasattr(self, "taskData"):
            saveDat["taskData"] = self.taskData.reset_index().to_dict("list")
        if hasattr(self, "accumData"):
            saveDat["accumData"] = self.accumData.reset_index().to_dict("list")
        if self.existsSQL:
            saveDat["sqlData"] = self.sqlData.reset_index().to_dict("list")
        if self.existsExecutors:
            saveDat["executors"] = self.executorData.reset_index().to_dict("list")

        saveDat["metadata"] = self.metadata

        if (filepath is not None) and ("s3://" in filepath):
            self.save_to_s3(saveDat, filepath, compress)
        else:
            self.save_to_local(saveDat, filepath, compress)

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
        logging.info("Saved object locally to: %s" % (filepath))

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

        logging.info("Saved object to cloud: %s" % (key))

    def load(self, filepath=None):

        if (filepath is not None) and ("s3://" in filepath):
            saveDat = self.load_from_s3(filepath)
        else:
            saveDat = self.load_from_local(filepath)

        self.metadata = saveDat["metadata"]
        self.existsSQL = self.metadata.pop("existsSQL")
        self.existsExecutors = self.metadata.pop("existsExecutors")

        # This is for legacy support and should be removed after it is in production for a few
        # weeks. Introduced 3/9/2022 by SDG.
        if "sparkMetadata" in saveDat:
            self.sparkMetadata = saveDat.pop("sparkMetadata")

        # SPC113 - SDG
        # Because of the way jobData is created, if there are no job Events in the eventlog then the
        # correct fields will not exist. A second condition checking for the 'job_id' field is
        # necessary here to ensure this method will run if this is the case.
        #
        # Note: stageData is initialized differently so this same issue does not exist for that
        # structure. Furthermore, in the event that 'jobData' has no values within, 'stageData' will
        # also have no values and an invalidLog exception will be thrown during log validation
        # in SparkApplicaionAdvanced.
        if ("jobData" in saveDat) and ("job_id" in saveDat["jobData"]):
            self.jobData = pd.DataFrame.from_dict(saveDat["jobData"])
            self.jobData = self.jobData.set_index("job_id")

        if "stageData" in saveDat:
            self.stageData = pd.DataFrame.from_dict(saveDat["stageData"]).set_index("stage_id")
        if "taskData" in saveDat:
            self.taskData = pd.DataFrame.from_dict(saveDat["taskData"]).set_index("task_id")
        if "accumData" in saveDat:
            self.accumData = pd.DataFrame.from_dict(saveDat["accumData"])
            if "sql_id" in self.accumData.columns:
                self.accumData = self.accumData.set_index("sql_id")

        if self.existsSQL:
            self.sqlData = pd.DataFrame.from_dict(saveDat["sqlData"]).set_index("sql_id")
        if self.existsExecutors:
            self.executorData = pd.DataFrame.from_dict(saveDat["executors"]).set_index(
                "executor_id"
            )

        logging.info("Loaded object from: %s" % (filepath))

        return self

    def load_from_s3(self, filepath):
        s3 = boto3.resource("s3")
        path = filepath.replace("s3://", "").split("/")
        bucket = path[0]
        key = ("/".join(path[1:])).lstrip("/")

        saveDat = s3.Object(bucket, key).get()["Body"].read()

        if ".gz" in filepath:
            saveDat = json.loads(gzip.decompress(saveDat).decode("utf-8"))
        else:
            saveDat = json.loads(saveDat.decode("ascii"))

        return saveDat

    def load_from_local(self, filepath):
        if ".gz" in filepath:
            with gzip.open(filepath, "r") as fin:
                saveDat = json.loads(fin.read().decode("ascii"))
        else:
            with open(filepath, "r") as fin:
                saveDat = json.loads(fin.read())

        return saveDat

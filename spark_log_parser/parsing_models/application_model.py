import collections
import gzip
import os

import boto3
import numpy
import ujson as json

from .dag_model import DagModel
from .exceptions import UrgentEventValidationException
from .executor_model import ExecutorModel
from .job_model import JobModel
from .stage_model import StageModel
from .task_model import TaskModel


def get_json(line):
    # Need to first strip the trailing newline, and then escape newlines (which can appear
    # in the middle of some of the JSON) so that JSON library doesn't barf.
    return json.loads(line.strip("\n").replace("\n", "\\n"))


class ApplicationModel:
    """
    Model for a spark application. A spark application consists of one or more jobs, which consists of one or more stages, which consists of one or more tasks.

    Spark application parameters can be parsed from an event log.

    Using parts of the trace analyzer from Kay Ousterhout: https://github.com/kayousterhout/trace-analysis

    """

    def __init__(self, eventlogpath, bucket=None, stdoutpath=None, debug=False):  # noqa: C901
        # set default parameters
        self.eventlogpath = eventlogpath
        self.dag = DagModel()
        self.jobs: dict[JobModel] = collections.defaultdict(JobModel)
        self.stages: dict[StageModel] = collections.defaultdict(StageModel)
        self.tasks: list[TaskModel] = []
        self.sql = collections.defaultdict(dict)
        self.accum_metrics = collections.defaultdict(dict)
        self.executors: dict[ExecutorModel] = collections.defaultdict(ExecutorModel)
        self.jobs_for_stage = collections.defaultdict(list)
        self.num_executors = 0
        self.max_executors = 0
        self.executorRemovedEarly = False
        self.parallelism = None
        self.memory_per_executor = None
        self.cores_per_executor = None
        self.num_instances = None
        self.start_time = None
        self.finish_time = None
        self.platformIdentified = False
        self.platform = None
        self.spark_metadata = {}
        self.stdoutpath = stdoutpath

        self.shuffle_partitions = 200

        self.cloud_platform = None
        self.cloud_provider = None
        self.cluster_id = None
        self.spark_version = None
        self.emr_version_tag = None

        # if bucket is None, then files are in local directory, else read from s3
        # read event log

        # Todo:  2022-03-26 RW:  Consider centralizing file access for easier error handling

        if bucket is None:

            # 2022-03-26  RW  Todo:  These file accesses will fail with invalid URL
            if ".gz" in eventlogpath:
                f = gzip.open(eventlogpath, "rt")
            else:
                f = open(eventlogpath, "r")
            test_line = f.readline()
            # print(get_json(test_line))

        else:
            s3_resource = boto3.resource("s3")
            bucket = s3_resource.Bucket(bucket)
            key = eventlogpath
            obj = bucket.Object(key=key)
            # 2022-03-26  RW  Todo:  This will fail with invalid URL
            response = obj.get()

            if ".gz" in key:
                filestring = gzip.decompress(response["Body"].read()).decode("utf-8")
            else:
                filestring = response["Body"].read().decode("utf-8")

            f = filestring.splitlines(True)
            test_line = f[0]

        try:
            get_json(test_line)
            is_json = True
            # print("Parsing file %s as JSON" % eventlogpath)
        except Exception:
            is_json = False
            print("Not json file. Check eventlog.")

        if bucket is None:
            f.seek(0)

        hosts = set()

        for line in f:
            if is_json:
                json_data = get_json(line)
                event_type = json_data["Event"]
                if event_type == "SparkListenerLogStart":
                    # spark_version_dict = {"spark_version": json_data["Spark Version"]}
                    self.spark_version = json_data["Spark Version"]
                    self.spark_metadata = {**self.spark_metadata}

                elif event_type == "SparkListenerJobStart":

                    job_id = json_data["Job ID"]
                    self.jobs[job_id].submission_time = json_data["Submission Time"] / 1000
                    # Avoid using "Stage Infos" here, which was added in 1.2.0.
                    stage_ids = json_data["Stage IDs"]

                    # print("Stage ids: %s" % stage_ids)
                    for stage_id in stage_ids:
                        self.jobs_for_stage[stage_id].append(job_id)

                elif event_type == "SparkListenerJobEnd":
                    job_id = json_data["Job ID"]
                    self.jobs[job_id].completion_time = json_data["Completion Time"] / 1000
                    self.jobs[job_id].result = json_data["Job Result"]["Result"]

                elif event_type == "SparkListenerTaskEnd":
                    if "Task Metrics" in json_data:
                        task = TaskModel(json_data, True)
                        self.tasks.append(task)

                elif event_type == "SparkListenerStageSubmitted":
                    stage_info = json_data["Stage Info"]

                    if "Submission Time" not in stage_info:
                        # PROD-426 Submission Time key may be missing from stages that
                        # don't get submitted. There is usually a StageCompleted event
                        # shortly after. This may happen when stages fail
                        continue

                    stage_id = stage_info["Stage ID"]
                    attempt_id = stage_info["Stage Attempt ID"]
                    stage = self.stages[stage_id]
                    # Note - see StageModel.attempt_id for a description of why this logic is here.
                    if stage.attempt_id is None or attempt_id >= stage.attempt_id:
                        stage.id = stage_id
                        stage.attempt_id = attempt_id
                        stage.stage_info = stage_info
                        stage.stage_name = stage_info["Stage Name"]
                        stage.submission_time = stage_info["Submission Time"] / 1000
                        stage.num_tasks = stage_info["Number of Tasks"]

                elif event_type == "SparkListenerStageCompleted":
                    # stages may not be executed exclusively from one job
                    stage_info = json_data["Stage Info"]
                    stage_id = stage_info["Stage ID"]
                    stage_completion_time = stage_info["Completion Time"] / 1000

                    stage = self.stages[stage_id]
                    attempt_id = stage_info["Stage Attempt ID"]
                    # Note - see StageModel.attempt_id for a description of why this logic is here.
                    if stage.attempt_id is None or attempt_id >= stage.attempt_id:
                        stage.completion_time = stage_completion_time
                        self.maybe_set_new_finish_time(stage_completion_time)

                elif event_type == "SparkListenerEnvironmentUpdate":

                    spark_properties = json_data["Spark Properties"]

                    # This if is specifically for databricks logs
                    if spark_version := spark_properties.get(
                        "spark.databricks.clusterUsageTags.sparkVersion"
                    ):
                        self.cloud_platform = "databricks"
                        self.spark_version = spark_version
                        self.cluster_id = spark_properties[
                            "spark.databricks.clusterUsageTags.clusterId"
                        ]
                        self.cloud_provider = spark_properties[
                            "spark.databricks.clusterUsageTags.cloudProvider"
                        ].lower()
                    elif cluster_id := json_data.get("System Properties", {}).get("EMR_CLUSTER_ID"):
                        self.cloud_platform = "emr"
                        self.cloud_provider = "aws"
                        self.cluster_id = cluster_id
                        self.emr_version_tag = json_data["System Properties"]["EMR_RELEASE_LABEL"]

                    self.spark_metadata = {**self.spark_metadata, **spark_properties}

                elif event_type == "SparkListenerExecutorAdded":
                    executor_id = json_data["Executor ID"]
                    executor_info = json_data["Executor Info"]

                    executor = self.executors[executor_id]
                    executor.id = executor_id
                    executor.start_time = json_data["Timestamp"]
                    executor.host = executor_info["Host"]
                    executor.cores = int(executor_info["Total Cores"])

                    hosts.add(executor.host)

                    # TODO - what happens if we receive multiple of these events and they have different
                    #  amounts of cores?
                    self.cores_per_executor = executor.cores
                    self.num_executors += 1
                    self.max_executors = max(self.num_executors, self.max_executors)

                # So far logs I've looked at only explicitly remove Executors when there is a problem like
                # lost worker. Use this to flag premature executor removal
                elif event_type == "SparkListenerExecutorRemoved":
                    self.executorRemovedEarly = True
                    self.num_executors = self.num_executors - 1

                    executor = self.executors[json_data["Executor ID"]]
                    executor.end_time = json_data["Timestamp"]
                    executor.removed_reason = json_data["Removed Reason"]

                elif event_type == "SparkListenerApplicationStart":
                    self.start_time = json_data["Timestamp"] / 1000
                    self.app_name = json_data["App Name"]

                elif event_type == "SparkListenerApplicationEnd":
                    self.maybe_set_new_finish_time(json_data["Timestamp"] / 1000)

                elif (
                    event_type == "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart"
                ):

                    sql_id = json_data["executionId"]
                    self.sql[sql_id]["start_time"] = json_data["time"] / 1000
                    self.sql[sql_id]["description"] = json_data["description"]
                    self.parse_all_accum_metrics(json_data)

                elif event_type == "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd":
                    sql_id = json_data["executionId"]
                    end_time = json_data["time"] / 1000
                    self.sql[sql_id]["end_time"] = end_time
                    self.maybe_set_new_finish_time(end_time)

                elif (
                    event_type
                    == "org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate"
                ):
                    self.parse_all_accum_metrics(json_data)

                # populate accumulated metrics with updated values
                elif (
                    event_type
                    == "org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates"
                ):
                    sql_id = json_data["executionId"]
                    for metric in json_data["accumUpdates"]:
                        accum_id = metric[0]
                        self.accum_metrics[accum_id]["value"] = metric[1]
                        self.accum_metrics[accum_id]["sql_id"] = sql_id

                elif (
                    event_type == "org.apache.spark.sql.execution.ui.SparkListenerEffectiveSQLConf"
                ):
                    #########################
                    # Note to predictor team:
                    # This is spark parameters for each sql execution id
                    # Need to decide how we want to deal with this.
                    # Right now the last one is saved, but need to figure out if we want to deal with each execution id's parameters
                    #########################
                    self.spark_metadata = {**self.spark_metadata, **json_data["effectiveSQLConf"]}

                # Add DAG components
                # using stage submitted to preserve order stages are submitted
                if event_type in ["SparkListenerJobStart", "SparkListenerStageSubmitted"]:
                    self.dag.parse_dag(json_data)

            else:
                # The file will only contain information for one job.
                self.jobs[0].add_event(line, False)

        if not self.cloud_platform:
            # Ideally, we would be able to determine the platform/provider reliably from our Spark logs. However, EMR
            # logs may not necessarily contain an SparkListenerEnvironmentUpdate event, so if we don't encounter one,
            # we will assume this is an EMR job running on AWS
            self.cloud_platform = "emr"
            self.cloud_provider = "aws"

        self.num_instances = len(hosts)
        self.executors_per_instance = numpy.ceil(self.num_executors / self.num_instances)

        for task in self.tasks:
            stage_id = task.stage_id
            stage = self.stages[stage_id]
            stage.add_task(task)

        for stage_id, stage in self.stages.items():
            if stage.submission_time is None:
                raise UrgentEventValidationException(missing_event=f"Stage {stage_id} Submit")

            job_ids_for_stage = self.jobs_for_stage.get(stage_id)
            if (not job_ids_for_stage) and (not debug):
                # If this stage is not associated with any particular job, that likely means we are missing some data
                raise UrgentEventValidationException(
                    missing_event=f"Job Start for Stage {stage_id}"
                )

            for job_id in job_ids_for_stage:
                self.jobs[job_id].stages[stage_id] = stage

            stage.finalize_tasks()

        for job_id, job in self.jobs.items():
            job.initialize_job()

        self.dag.decipher_dag()
        self.dag.add_broadcast_dependencies(self.stdoutpath)

    def maybe_set_new_finish_time(self, new_finish_time: int):
        """
        As we read log lines, we want to mark the 'latest' finish_time we have seen. This may come from a few different
        events, since there is no single event that is guaranteed to be present in the log from which we can determine
        the application's finish_time
        """
        if not self.finish_time or new_finish_time > self.finish_time:
            self.finish_time = new_finish_time

    def plot_task_runtime_distribution(self):
        """
        For each stage, plot task runtime distribution and calculate how closely it follows normal distribution

        Returns true if normal distribution
        """
        # TODO: fill in plotting and calculation
        return

    # Accumulated metrics including broadcast data
    def parse_all_accum_metrics(self, accum_data):
        # Search recursively for accumulated metrics (can be many layers deep)
        for k, v in accum_data.items():
            if k == "metrics":
                for metric in v:
                    accum_id = metric["accumulatorId"]
                    self.accum_metrics[accum_id]["name"] = metric["name"]
                    self.accum_metrics[accum_id]["metric_type"] = metric["metricType"]
            if isinstance(v, dict):
                self.parse_all_accum_metrics(v)
            if isinstance(v, list):
                for d in v:
                    if isinstance(d, dict):
                        self.parse_all_accum_metrics(d)

import collections
import json
import logging
import numpy

import os
import boto3
import gzip
from pprint import pprint

from .job_model import JobModel
from .executor_model import ExecutorModel
from .dag_model import DagModel

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

    def __init__(self, eventlogpath, bucket=None, stdoutpath=None):
        # set default parameters
        self.eventlogpath = eventlogpath
        self.dag  = DagModel()
        self.jobs = collections.defaultdict(JobModel)
        self.sql  = collections.defaultdict(dict)
        self.accum_metrics = collections.defaultdict(dict)
        self.executors = collections.defaultdict(ExecutorModel)
        self.jobs_for_stage = {}
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
        self.spark_version = None

        # if bucket is None, then files are in local directory, else read from s3
        # read event log

        # 2022-03-26 RW:  Consider centralizing file access for easier error handling

        if bucket is None:

            # 2022-03-26  RW  Todo:  These file accesses will fail with invalid URL
            if '.gz' in eventlogpath:
                f = gzip.open(eventlogpath, "rt")
            else:
                f = open(eventlogpath, "r")
            test_line = f.readline()
            # print(get_json(test_line))

        else:
            s3_resource = boto3.resource('s3')
            bucket = s3_resource.Bucket(bucket)
            key = eventlogpath
            obj = bucket.Object(key=key)
            # 2022-03-26  RW  Todo:  This will fail with invalid URL
            response = obj.get()

            if '.gz' in key:
                filestring = gzip.decompress(response['Body'].read()).decode('utf-8')
            else:
                filestring = response['Body'].read().decode('utf-8')
                
            f = filestring.splitlines(True) 
            test_line = f[0] 

        try:
            get_json(test_line)
            is_json = True
            # print("Parsing file %s as JSON" % eventlogpath)
        except:
            is_json = False
            print("Not json file. Check eventlog.")
        
        if bucket is None:
            f.seek(0)

        hosts = []

        for line in f:
            if is_json:
                json_data = get_json(line)
                event_type = json_data["Event"]
                if event_type == "SparkListenerLogStart":
                    #spark_version_dict = {"spark_version": json_data["Spark Version"]}
                    self.spark_version = json_data["Spark Version"]
                    self.spark_metadata = {**self.spark_metadata}
                elif event_type == "SparkListenerJobStart":
                   
                    job_id = json_data["Job ID"]
                    self.jobs[job_id].submission_time = json_data['Submission Time']/1000
                    # Avoid using "Stage Infos" here, which was added in 1.2.0.
                    stage_ids = json_data["Stage IDs"]
                    
                    # print("Stage ids: %s" % stage_ids)
                    for stage_id in stage_ids:
                        if stage_id not in self.jobs_for_stage:
                            self.jobs_for_stage[stage_id] = [job_id]
                        else:
                            self.jobs_for_stage[stage_id].append(job_id)

                elif event_type == "SparkListenerJobEnd":
                    job_id = json_data["Job ID"]
                    self.jobs[job_id].completion_time = json_data['Completion Time']/1000
                    self.jobs[job_id].result = json_data['Job Result']['Result']
                elif event_type == "SparkListenerTaskEnd":
                    stage_id = json_data["Stage ID"]
                    # Add the event to all of the jobs that depend on the stage.
                    for jid in self.jobs_for_stage[stage_id]:
                        self.jobs[jid].add_event(json_data, True)
                     
                elif event_type == "SparkListenerStageSubmitted":

                    # stages may not be executed exclusively from one job
                    stage_id = json_data['Stage Info']["Stage ID"]
                    for job_id in self.jobs_for_stage[stage_id]:
                        self.jobs[job_id].stages[stage_id].submission_time = json_data['Stage Info']['Submission Time']/1000
                        self.jobs[job_id].stages[stage_id].stage_name = json_data['Stage Info']['Stage Name']
                        self.jobs[job_id].stages[stage_id].num_tasks = json_data['Stage Info']['Number of Tasks']
                        self.jobs[job_id].stages[stage_id].stage_info = json_data['Stage Info']

                elif event_type == "SparkListenerStageCompleted":
                
                    # stages may not be executed exclusively from one job
                    stage_id = json_data['Stage Info']["Stage ID"]
                    self.finish_time = json_data['Stage Info']['Completion Time']/1000

                    for job_id in self.jobs_for_stage[stage_id]:
                        self.jobs[job_id].stages[stage_id].completion_time = json_data['Stage Info']['Completion Time']/1000


                elif event_type == "SparkListenerEnvironmentUpdate":

                    curKeys = json_data["Spark Properties"].keys()

                    # This if is specifically for databricks logs
                    if 'spark.databricks.clusterUsageTags.sparkVersion' in curKeys:

                        self.cloud_platform = 'databricks'
                        self.cloud_provider = json_data['Spark Properties']['spark.databricks.clusterUsageTags.cloudProvider'].lower()
                        self.spark_version = json_data['Spark Properties']['spark.databricks.clusterUsageTags.sparkVersion']

                    self.spark_metadata = {**self.spark_metadata, **json_data["Spark Properties"]}

                    ##################################
                    # Note to predictor team:
                    # Keeping these for now so nothing breaks, but adding transferring the entire Spark Properties dictionary above
                    # so we can see what has been set and don't have to manually grab every property. Should be able to remove the 
                    # section below once we make minor tweaks to predictor.
                    ##################################

                    # if 'spark.executor.instances' in curKeys:
                    #     self.num_executors = int(json_data["Spark Properties"]["spark.executor.instances"])
                    if 'spark.default.parallelism' in curKeys:
                        self.parallelism = int(json_data["Spark Properties"]["spark.default.parallelism"])
                    if 'spark.executor.memory' in curKeys:
                        self.memory_per_executor = json_data["Spark Properties"]["spark.executor.memory"]                   
                    #if 'spark.executor.cores' in curKeys:
                    #    self.cores_per_executor = int(json_data["Spark Properties"]["spark.executor.cores"])
                    if 'spark.sql.shuffle.partitions' in curKeys:
                        self.shuffle_partitions = int(json_data["Spark Properties"]["spark.sql.shuffle.partitions"])

                elif event_type == "SparkListenerExecutorAdded":
                    hosts.append(json_data["Executor Info"]["Host"])
                    self.cores_per_executor =  int(json_data["Executor Info"]["Total Cores"])
                    self.num_executors = self.num_executors+1
                    self.max_executors = max(self.num_executors, self.max_executors)
                    self.executors[int(json_data['Executor ID'])] = ExecutorModel(json_data)
                    self.executors[int(json_data['Executor ID'])].removed_reason = ''

                # So far logs I've looked at only explicitly remove Executors when there is a problem like
                # lost worker. Use this to flag premature executor removal
                elif event_type == "SparkListenerExecutorRemoved":
                    self.executorRemovedEarly = True
                    self.num_executors = self.num_executors-1
                    self.executors[int(json_data['Executor ID'])].end_time = json_data['Timestamp']
                    self.executors[int(json_data['Executor ID'])].removed_reason = json_data['Removed Reason']

                elif event_type == "SparkListenerApplicationStart":
                    self.start_time = json_data["Timestamp"]/1000
                    self.app_name = json_data["App Name"]

                elif event_type == "SparkListenerApplicationEnd":
                    self.finish_time = json_data["Timestamp"]/1000

                elif event_type == "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart":

                    sql_id = json_data['executionId']
                    self.sql[sql_id]['start_time']  = json_data['time']/1000
                    self.sql[sql_id]['description'] = json_data['description']
                    self.parse_all_accum_metrics(json_data)

                elif event_type == "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd":
                    sql_id = json_data['executionId']
                    self.sql[sql_id]['end_time']= json_data['time']/1000
                    self.finish_time = json_data["time"]/1000

                elif event_type == "org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate":
                    self.parse_all_accum_metrics(json_data)    

                # populate accumulated metrics with updated values
                elif event_type == "org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates":
                    sql_id = json_data['executionId']
                    for metric in json_data['accumUpdates']:
                        accum_id = metric[0]
                        self.accum_metrics[accum_id]['value'] = metric[1]     
                        self.accum_metrics[accum_id]['sql_id'] = sql_id       

                elif event_type == "org.apache.spark.sql.execution.ui.SparkListenerEffectiveSQLConf":
                    #########################
                    # Note to predictor team:
                    # This is spark parameters for each sql execution id
                    # Need to decide how we want to deal with this. 
                    # Right now the last one is saved, but need to figure out if we want to deal with each execution id's parameters
                    #########################
                    self.spark_metadata = {**self.spark_metadata, **json_data["effectiveSQLConf"]}          
                    
                # Add DAG components
                # using stage submitted to preserve order stages are submitted
                if event_type in ['SparkListenerJobStart','SparkListenerStageSubmitted']:
                    self.dag.parse_dag(json_data)

            else:
                # The file will only contain information for one job.
                self.jobs[0].add_event(line, False)


        if self.cloud_platform == None:
            self.cloud_platform = 'emr'
            self.cloud_provider = 'aws'
            #self.spark_metadata['cloud_platform'] = 'emr'
            #self.spark_metadata['cloud_provider'] = 'aws'

        self.dag.decipher_dag()
        self.dag.add_broadcast_dependencies(self.stdoutpath)

        self.num_instances = len(numpy.unique(hosts))
        self.executors_per_instance = numpy.ceil(self.num_executors/self.num_instances)

        # print("Finished reading input data:")
        for job_id, job in self.jobs.items():
            job.initialize_job()
            # print("Job", job_id, " has stages: ", job.stages.keys())

    def output_all_job_info(self):
        for job_id, job in self.jobs.items():
            features_filename = f'{os.path.dirname(self.eventlogpath)}/e{self.num_executors}_p{self.parallelism}_mem{self.memory_per_executor}_job{job_id}'
            job.write_features(features_filename)            


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
            if k == 'metrics':
                for metric in v:        
                    accum_id = metric['accumulatorId']
                    self.accum_metrics[accum_id]['name'] = metric['name']
                    self.accum_metrics[accum_id]['metric_type'] = metric['metricType']     
            if isinstance(v, dict):
                self.parse_all_accum_metrics(v)
            if isinstance(v, list):
                for d in v:
                    if isinstance(d, dict):
                        self.parse_all_accum_metrics(d)                          



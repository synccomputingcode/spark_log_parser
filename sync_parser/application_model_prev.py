import collections
import json
import logging
import numpy

import os
import boto3
import gzip

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

    def __init__(self, eventlogpath, bucket=None):
        # set default parameters
        self.eventlogpath = eventlogpath
        self.dag  = DagModel()
        self.jobs = collections.defaultdict(JobModel)
        self.sql  = collections.defaultdict(dict)
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

        self.shuffle_partitions = 200

        # if bucket is None, then files are in local directory, else read from s3
        # read event log
        if bucket is None:

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
            response = obj.get()
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
                if event_type == "SparkListenerJobStart":
                   
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
                    for job_id in self.jobs_for_stage[stage_id]:
                        self.jobs[job_id].add_event(json_data, True)
                elif event_type == "SparkListenerStageCompleted":
                    #print(json_data)
                    stage_id = json_data['Stage Info']["Stage ID"]
                    self.jobs[job_id].stages[stage_id].submission_time = json_data['Stage Info']['Submission Time']/1000
                    self.jobs[job_id].stages[stage_id].completion_time = json_data['Stage Info']['Completion Time']/1000
                    self.jobs[job_id].stages[stage_id].stage_name = json_data['Stage Info']['Stage Name']
                    self.jobs[job_id].stages[stage_id].num_tasks = json_data['Stage Info']['Number of Tasks']
                elif event_type == "SparkListenerEnvironmentUpdate":

                    curKeys = json_data["Spark Properties"].keys()

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

                # So far logs I've looked at only explicitly remove Executors when there is a problem like
                # lost worker. Use this to flag premature executor removal
                elif event_type == "SparkListenerExecutorRemoved":
                    self.executorRemovedEarly = True
                    self.num_executors = self.num_executors-1
                    self.executors[int(json_data['Executor ID'])].end_time = json_data['Timestamp']

                elif event_type == "SparkListenerApplicationStart":
                    self.start_time = json_data["Timestamp"]/1000

                elif event_type == "SparkListenerApplicationEnd":
                    self.finish_time = json_data["Timestamp"]/1000

                elif event_type == "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart":
                    sql_id = json_data['executionId']
                    self.sql[sql_id]['start_time']  = json_data['time']/1000
                    self.sql[sql_id]['description'] = json_data['description']

                elif event_type == "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd":
                    sql_id = json_data['executionId']
                    self.sql[sql_id]['end_time']= json_data['time']/1000
                    self.finish_time = json_data["time"]/1000

                elif event_type == "org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates":
                    self.driver_accum_updates = json_data['accumUpdates']
                    
                # Add DAG components
                if event_type in ['SparkListenerJobStart','SparkListenerStageCompleted']:
                    self.dag.parse_dag(json_data)

            else:
                # The file will only contain information for one job.
                self.jobs[0].add_event(line, False)


            if self.platformIdentified == False:
                if 'EMR_RELEASE_LABEL' in line:
                    self.platform = 'emr'
                    self.platformIdentified = True
                elif 'databricks' in line:
                    self.platform = 'databricks'
                    self.platformIdentified = True

        self.dag.decipher_dag()

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

class JobModel:
    """ 
    Model for a job within an application.
    
    Modified from trace analyzer from Kay Ousterhout: https://github.com/kayousterhout/trace-analysis

    """
    def __init__(self):
        self.logger = logging.getLogger("Job")
        # Map of stage IDs to Stages.
        self.stages = collections.defaultdict(StageModel)

    def add_event(self, data, is_json):
        if is_json:
            event_type = data["Event"]
            if event_type == "SparkListenerTaskEnd":
                stage_id = data["Stage ID"]
                self.stages[stage_id].add_event(data, True)
        else:
            STAGE_ID_MARKER = "STAGE_ID="
            stage_id_loc = data.find(STAGE_ID_MARKER)
            if stage_id_loc != -1:
                stage_id_and_suffix = data[stage_id_loc + len(STAGE_ID_MARKER):]
                stage_id = stage_id_and_suffix[:stage_id_and_suffix.find(" ")]
                self.stages[stage_id].add_event(data, False)

    def initialize_job(self):
        """ Should be called after adding all events to the job. """
        # Drop empty stages.
        stages_to_drop = []
        for id, s in self.stages.items():
            if len(s.tasks) == 0:
                stages_to_drop.append(id)
        for id in stages_to_drop:
            # print("Dropping stage %s" % id)
            del self.stages[id]

        # Compute the amount of overlapped time between stages
        # (there should just be two stages, at the beginning, that overlap and run concurrently).
        # This computation assumes that not more than two stages overlap.
        # print(["%s: %s tasks" % (id, len(s.tasks)) for id, s in self.stages.items()])
        start_and_finish_times = [(id, s.start_time, s.conservative_finish_time())
            for id, s in self.stages.items()]
        start_and_finish_times.sort(key = lambda x: x[1])
        self.overlap = 0
        old_end = 0
        previous_id = ""
        self.stages_to_combine = set()
        for id, start, finish in start_and_finish_times:
            if start < old_end:
                self.overlap += old_end - start
                # print("Overlap:", self.overlap, "between ", id, "and", previous_id)
                self.stages_to_combine.add(id)
                self.stages_to_combine.add(previous_id)
                old_end = max(old_end, finish)
            if finish > old_end:
                old_end = finish
                previous_id = id

        # print("Stages to combine: ", self.stages_to_combine)

        # self.combined_stages_concurrency = -1
        # if len(self.stages_to_combine) > 0:
        #     tasks_for_combined_stages = []
        #     for stage_id in self.stages_to_combine:
        #         tasks_for_combined_stages.extend(self.stages[stage_id].tasks)
        #         self.combined_stages_concurrency = concurrency.get_max_concurrency(tasks_for_combined_stages)

    def all_tasks(self):
        """ Returns a list of all tasks. """
        return [task for stage in self.stages.values() for task in stage.tasks]

    def write_features(self, filepath):
        """ Outputs a csv file with features of each task """
        with open(f'{filepath}.csv', 'w') as f:
            f.write('stage_id, executor_id, start_time, finish_time, executor, executor_run_time, executor_deserialize_time, result_serialization_time, gc_time, network_bytes_transmitted_ps, network_bytes_received_ps, process_cpu_utilization, total_cpu_utilization, shuffle_write_time, shuffle_mb_written, input_read_time, input_mb, output_mb, has_fetch, data_local, local_mb_read, local_read_time, total_time_fetching, remote_mb_read, scheduler_delay\n')
            for id, stage in self.stages.items():
                numpy.savetxt(f, stage.get_features(id), delimiter=',', fmt="%s")    


class StageModel:
    """
    Model for a stage within a spark job
    """
    def __init__(self):
        self.start_time = -1
        self.tasks = []

    
    def average_task_runtime(self):
        return numpy.percentile([t.compute_time() for t in self.tasks], 50) if len(self.tasks) > 0 else 0

    def average_executor_deserialize_time(self):
        return sum([t.executor_deserialize_time for t in self.tasks]) * 1.0 / len(self.tasks) if len(self.tasks) > 0 else 0

    def average_result_serialization_time(self):
        return sum([t.result_serialization_time for t in self.tasks]) * 1.0 / len(self.tasks) if len(self.tasks) > 0 else 0  

    def total_executor_deserialize_time(self):
        return sum([t.executor_deserialize_time for t in self.tasks]) 

    def total_result_serialization_time(self):
        return sum([t.result_serialization_time for t in self.tasks])     

    def total_scheduler_delay(self):
        return sum([t.scheduler_delay for t in self.tasks])     

    def total_peak_execution_memory(self):
        return sum([t.peak_execution_memory for t in self.tasks])            

    def average_scheduler_delay(self):
        return sum([t.scheduler_delay for t in self.tasks]) * 1.0 / len(self.tasks) if len(self.tasks) > 0 else 0

    def average_gc_time(self):
        return sum([t.gc_time for t in self.tasks]) * 1.0 / len(self.tasks) if len(self.tasks) > 0 else 0

    def total_gc_time(self):
        return sum([t.gc_time for t in self.tasks]) 

    def has_shuffle_read(self):
        total_shuffle_read_bytes = sum(
            [t.remote_mb_read + t.local_mb_read for t in self.tasks if t.has_fetch])
        return total_shuffle_read_bytes > 0

    def conservative_finish_time(self):
        # Subtract scheduler delay to account for asynchrony in the scheduler where sometimes tasks
        # aren't marked as finished until a few ms later.
        return max([(t.finish_time - t.scheduler_delay) for t in self.tasks])

    def finish_time(self):
        return max([t.finish_time for t in self.tasks], default=0)   

    def total_runtime(self):
        return sum([t.finish_time - t.start_time for t in self.tasks])

    def total_fetch_wait(self):
        return sum([t.fetch_wait for t in self.tasks if t.has_fetch])

    def total_remote_blocks_read(self):
        return sum([t.remote_blocks_read for t in self.tasks if t.has_fetch])     

    def total_remote_mb_read(self):
        return sum([t.remote_mb_read for t in self.tasks if t.has_fetch])     

    def total_write_time(self):
        return sum([t.shuffle_write_time for t in self.tasks])
        
    def total_memory_bytes_spilled(self):
        return sum([t.memory_bytes_spilled for t in self.tasks])

    def total_disk_bytes_spilled(self):
        return sum([t.disk_bytes_spilled for t in self.tasks])        

    def max_disk_bytes_spilled(self):
        return max([t.disk_bytes_spilled for t in self.tasks])      

    def max_memory_bytes_spilled(self):
        return max([t.memory_bytes_spilled for t in self.tasks]) 
  
    def max_task_runtime(self):
        return numpy.percentile([t.compute_time() for t in self.tasks], 100) if len(self.tasks) > 0 else 0          

    def max_peak_execution_memory(self):
        return max([t.peak_execution_memory for t in self.tasks]) 

    def total_runtime_no_remote_shuffle_read(self):
        return sum([t.runtime_no_remote_shuffle_read() for t in self.tasks])

    def total_time_fetching(self):
        return sum([t.total_time_fetching for t in self.tasks if t.has_fetch])

    def input_mb(self):
        """ Returns the total input size for this stage.

        This is only valid if the stage read data from a shuffle.
        """
        total_input_bytes = sum([t.remote_mb_read + t.local_mb_read for t in self.tasks if t.has_fetch])
        total_input_bytes += sum([t.input_mb for t in self.tasks])
        return total_input_bytes

    def output_mb(self):
        """ Returns the total output size for this stage.

        This is only valid if the output data was written for a shuffle.
        TODO: Add HDFS / in-memory RDD output size.
        """
        total_output_size = sum([t.shuffle_mb_written for t in self.tasks])
        return total_output_size

    def add_event(self, data, is_json):
        # TODO: account for failed tasks
        if "Task Metrics" in data:
            task = TaskModel(data, is_json)

            if self.start_time == -1:
                self.start_time = task.start_time
            else:
                self.start_time = min(self.start_time, task.start_time)

            self.tasks.append(task)

    def get_features(self, stage_id):
        """ Return features from stage to write to csv

        """
        features = []
        for t in self.tasks:
            features.append([stage_id, t.executor_id, t.start_time, t.finish_time, t.executor, t.executor_run_time, t.executor_deserialize_time, t.result_serialization_time, t.gc_time, t.network_bytes_transmitted_ps, t.network_bytes_received_ps, t.process_cpu_utilization, t.total_cpu_utilization, t.shuffle_write_time, t.shuffle_mb_written, t.input_read_time, t.input_mb, t.output_mb, t.has_fetch, t.data_local, t.local_mb_read, t.local_read_time, t.total_time_fetching, t.remote_mb_read, t.scheduler_delay])

        # print(features)
        return features

class TaskModel:
    """
    Model for a task within a stage
    """
    def __init__(self, data, is_json):
        if is_json:
            self.initialize_from_json(data)
        else:
            self.initialize_from_job_logger(data)

        self.scheduler_delay = (self.finish_time - self.executor_run_time -
            self.executor_deserialize_time - self.result_serialization_time - self.start_time)
        # Should be set to true if this task is a straggler and we know the cause of the
        # straggler behavior.
        self.straggler_behavior_explained = False
    

    def initialize_from_json(self, json_data):

        self.logger = logging.getLogger("Task")

        task_info = json_data["Task Info"]
        
        task_metrics = json_data["Task Metrics"]
        if "Task Executor Metrics" in json_data:
            task_executor_metrics = json_data["Task Executor Metrics"]
        else:
            task_executor_metrics = None

        self.start_time = task_info["Launch Time"]/1000 # [s]
        self.finish_time = task_info["Finish Time"]/1000 # [s]
        self.task_id     = task_info['Task ID']
        self.executor = task_info["Host"]
        self.executor_run_time = task_metrics["Executor Run Time"]/1000 # [s]
        self.executor_deserialize_time = task_metrics["Executor Deserialize Time"]/1000 # [s]
        self.result_serialization_time = task_metrics["Result Serialization Time"]/1000 # [s]#
        self.gc_time = task_metrics["JVM GC Time"]/1000 # [s]
        self.memory_bytes_spilled = task_metrics["Memory Bytes Spilled"]/1000000 # [MB]
        self.disk_bytes_spilled = task_metrics["Disk Bytes Spilled"]/1000000 # [MB]
        if "Peak Execution Memory" in task_metrics:
            self.peak_execution_memory = task_metrics["Peak Execution Memory"]/1000000 # [MB]
        else:
            self.peak_execution_memory = -1
        # TODO: looks like this is never used.
        self.executor_id = task_info["Executor ID"]

        # TODO: add utilization to task metrics output by JSON.
        self.disk_utilization = {}
        self.network_bytes_transmitted_ps = 0
        self.network_bytes_received_ps = 0
        self.process_cpu_utilization = 0
        self.total_cpu_utilization = 0

        self.shuffle_write_time = 0
        self.shuffle_mb_written = 0


        SHUFFLE_WRITE_METRICS_KEY = "Shuffle Write Metrics"
        if SHUFFLE_WRITE_METRICS_KEY in task_metrics:
            shuffle_write_metrics = task_metrics[SHUFFLE_WRITE_METRICS_KEY] 
            # Convert to s (from nanoseconds).
            self.shuffle_write_time = shuffle_write_metrics["Shuffle Write Time"] / 1.0e9
        OPEN_TIME_KEY = "Shuffle Open Time"
        if OPEN_TIME_KEY in shuffle_write_metrics:
            shuffle_open_time = shuffle_write_metrics[OPEN_TIME_KEY] / 1.0e9
            print("Shuffle open time: ", shuffle_open_time)
            self.shuffle_write_time += shuffle_open_time
        CLOSE_TIME_KEY = "Shuffle Close Time"
        if CLOSE_TIME_KEY in shuffle_write_metrics:
            shuffle_close_time = shuffle_write_metrics[CLOSE_TIME_KEY] / 1.0e9
            print("Shuffle close time: ", shuffle_close_time)
            self.shuffle_write_time += shuffle_close_time
        self.shuffle_mb_written = shuffle_write_metrics["Shuffle Bytes Written"] / 1048576.

        # TODO: print warning when non-zero disk bytes spilled??
        # TODO: are these accounted for in shuffle metrics?

        INPUT_METRICS_KEY = "Input Metrics"
        self.input_read_time = 0
        self.input_read_method = "unknown"
        self.input_mb = 0

        if INPUT_METRICS_KEY in task_metrics:
            input_metrics = task_metrics[INPUT_METRICS_KEY]
            self.input_read_time = 0 # TODO: fill in once input time has been added.
            # self.input_read_method = input_metrics["Data Read Method"]
            self.input_mb = input_metrics["Bytes Read"] / 1048576.

        # TODO: add write time and MB.
        self.output_write_time = 0 #int(items_dict["OUTPUT_WRITE_BLOCKED_NANOS"]) / 1.0e6
        self.output_mb = 0
        #if "OUTPUT_BYTES" in items_dict:
        #  self.output_mb = int(items_dict["OUTPUT_BYTES"]) / 1048576.

        self.has_fetch = True
        # False if the task was a map task that did not run locally with its input data.
        self.data_local = True
        SHUFFLE_READ_METRICS_KEY = "Shuffle Read Metrics"
        if SHUFFLE_READ_METRICS_KEY not in task_metrics:
            if (task_info["Locality"] != "NODE_LOCAL") and (task_info["Locality"] != "PROCESS_LOCAL"):
                self.data_local = False
            self.has_fetch = False
            return

        shuffle_read_metrics = task_metrics[SHUFFLE_READ_METRICS_KEY]
        
        self.fetch_wait = shuffle_read_metrics["Fetch Wait Time"]/1000 # [s]
        self.local_blocks_read = shuffle_read_metrics["Local Blocks Fetched"]
        self.remote_blocks_read = shuffle_read_metrics["Remote Blocks Fetched"]
        self.remote_mb_read = shuffle_read_metrics["Remote Bytes Read"] / 1048576.
        # if self.remote_mb_read > 0:
        #   print(f'remote mb read: {self.remote_mb_read}')

        self.local_mb_read = 0
        LOCAL_BYTES_READ_KEY = "Local Bytes Read"
        if LOCAL_BYTES_READ_KEY in shuffle_read_metrics:
            self.local_mb_read = shuffle_read_metrics[LOCAL_BYTES_READ_KEY] / 1048576.
            # The local read time is not included in the fetch wait time: the task blocks
            # on reading data locally in the BlockFetcherIterator.initialize() method.
        self.local_read_time = 0
        LOCAL_READ_TIME_KEY = "Local Read Time"
        if LOCAL_READ_TIME_KEY in shuffle_read_metrics:
            self.local_read_time = shuffle_read_metrics[LOCAL_READ_TIME_KEY]/1000 # [s]
        self.total_time_fetching = shuffle_read_metrics["Fetch Wait Time"]/1000 # [s]
            # if self.total_time_fetching > 0:
            #   print(f'shuffle wait time: {self.total_time_fetching}')
        if task_executor_metrics is not None:
            self.jvm_heap_memory = task_executor_metrics["JVMHeapMemory"]
            self.jvm_offheap_memory = task_executor_metrics["JVMOffHeapMemory"]
            self.onheap_execution_memory = task_executor_metrics["OnHeapExecutionMemory"]
            self.onheap_storage_memory = task_executor_metrics["OnHeapStorageMemory"]
            self.offheap_storage_memory = task_executor_metrics["OffHeapStorageMemory"]
            self.onheap_unified_memory = task_executor_metrics["OnHeapUnifiedMemory"]
            self.offheap_unified_memory = task_executor_metrics["OffHeapUnifiedMemory"]

    def input_size_mb(self):
        if self.has_fetch:
            return self.remote_mb_read + self.local_mb_read
        else:
            return self.input_mb

    def compute_time_without_gc(self):
        """ Returns the time this task spent computing.
        
        Assumes shuffle writes don't get pipelined with task execution (TODO: verify this).
        Does not include GC time.
        """
        compute_time = (self.runtime() - self.scheduler_delay - self.gc_time -
        self.shuffle_write_time - self.input_read_time - self.output_write_time)
        if self.has_fetch:
            # Subtract off of the time to read local data (which typically comes from disk) because
            # this read happens before any of the computation starts.
            compute_time = compute_time - self.fetch_wait - self.local_read_time
        return compute_time

    def compute_time(self):
        """ Returns the time this task spent computing (potentially including GC time).

        The reason we don't subtract out GC time here is that garbage collection may happen
        during fetch wait.
        """
        return self.compute_time_without_gc() + self.gc_time

    # Added by SDG 4/30/2021 for further breakdown of "compute" time
    def task_compute_time(self):
        task_compute_time = self.compute_time() - self.executor_deserialize_time - self.result_serialization_time
        return task_compute_time

    def runtime(self):
        return self.finish_time - self.start_time

    def runtime_no_input(self):
        new_finish_time = self.finish_time - self.input_read_time
        return new_finish_time - self.start_time

    def runtime_no_output(self):
        new_finish_time = self.finish_time - self.output_write_time
        return new_finish_time - self.start_time

    def runtime_no_shuffle_write(self):
        return self.finish_time - self.shuffle_write_time - self.start_time

    def runtime_no_shuffle_read(self):
        if self.has_fetch:
            return self.finish_time - self.fetch_wait - self.local_read_time - self.start_time
        else:
            return self.runtime()

    def runtime_no_remote_shuffle_read(self):
        if self.has_fetch:
            return self.finish_time - self.fetch_wait - self.start_time
        else:
            return self.runtime()

    def runtime_no_output(self):
        new_finish_time = self.finish_time - self.output_write_time
        return new_finish_time - self.start_time

    def runtime_no_input_or_output(self):
        new_finish_time = self.finish_time - self.input_read_time - self.output_write_time
        return new_finish_time - self.start_time

    def runtime_no_network(self):
        runtime_no_in_or_out = self.runtime_no_output()
        if not self.data_local:
            runtime_no_in_or_out -= self.input_read_time
        if self.has_fetch:
            return runtime_no_in_or_out - self.fetch_wait
        else:
            return runtime_no_in_or_out

class ExecutorModel:
    """
    Model for a task within a stage
    """
    def __init__(self, data):

        self.id          = data['Executor ID']
        self.start_time  = data['Timestamp']
        self.end_time    = None 
        self.host        = data['Executor Info']['Host']
        self.cores       = data['Executor Info']['Total Cores']
        


class DagModel:

    def __init__(self):

        self.debug = False
        self.stage_dict       = {} # <key="stage_id",     value="trans_id">
        self.alias_dict       = {} # <key="trans_id",     value=["stage_id"]>
        self.parents_dag_dict = {} # <key="parent_stage", value=["child_stage"]>
        self.rdd_stage_dict   = {} # <key="RDD_id",       value=["Stage_IDs"]>
        self.stage_rdd_dict   = {} # <key="stage_id",     value=["RDD_ids"]
        self.rdd_parent_dict  = {} # <key="RDD_id",       value=["RDD_parents"]>
        self.rdd_persist_dict = {} # <key="stage_id",     value="persist?">


    # Extract the DAG from the Spark log SparkListenerJobStart and SparkListenerStageStart
    def parse_dag(self, data):
        # Create the stage alias dictionaries
        # NOTE: SparkListenerJobStart will always be before SparkListenerStageStart
        if 'Stage Infos' in data.keys():
            stages = data['Stage Infos']
            for stage in stages:
                if self.debug: 
                    print("Stage " + str(stage["Stage ID"]) + \
                          " has parent(s) " + str(stage["Parent IDs"]))

                # Stage-level data
                stage_id                        = stage["Stage ID"]
                stage_parents                   = stage["Parent IDs"]
                self.rdd_persist_dict[stage_id] = {}
                rdd_dict                        = {}
                for x in stage["RDD Info"]:
                    if self.debug: 
                        print("\tRDD " + str(x["RDD ID"])            + \
                              " has parents " + str(x["Parent IDs"]) + \
                              " and is named " + str(x["Scope"]))
                    rdd_id      = x["RDD ID"]
                    rdd_parents = x["Parent IDs"]

                    # Keep track of whether this RDD was persisted
                    use_disk   = bool(x["Storage Level"]["Use Disk"])
                    use_memory = bool(x["Storage Level"]["Use Memory"])
                    self.rdd_persist_dict[stage_id][rdd_id] = use_disk or use_memory

                    # Keep track of RDDs for this stage
                    rdd_dict[rdd_id] = rdd_parents

                    # Keep track of RDD-level parents
                    if rdd_id not in self.rdd_parent_dict.keys():
                        self.rdd_parent_dict[rdd_id] = rdd_parents
                    else:
                        self.rdd_parent_dict[rdd_id] += rdd_parents

                    # Keep track of which RDD(s) belong to this stage
                    if stage_id not in self.stage_rdd_dict.keys():
                        self.stage_rdd_dict[stage_id] = [rdd_id]
                    else:
                        self.stage_rdd_dict[stage_id].append(rdd_id)

                    # Keep track of which Stage(s) an RDD belongs to
                    if rdd_id not in self.rdd_stage_dict.keys():
                        self.rdd_stage_dict[rdd_id] = [stage_id]
                    else:
                        self.rdd_stage_dict[rdd_id].append(stage_id)

                # Create an identifier for this stage and store aliases
                trans_id = str([int(x) for x in list(rdd_dict.keys())])
                if trans_id not in self.alias_dict.keys():
                    self.alias_dict[trans_id] = [stage_id]
                else:
                    self.alias_dict[trans_id].append(stage_id)
                
                # Save the dictionary of unique stage identifiers
                self.stage_dict[stage_id] = trans_id 

        # Stages will be processed in execution order.
        if 'Stage Info' in data.keys():
            stage_info    = data['Stage Info']
            stage_id      = stage_info['Stage ID']
            stage_parents = stage_info['Parent IDs']
            if self.debug: print("Stage " + str(stage_id) + " has parents " + str(stage_parents))

            # Setup parents
            if stage_id not in self.parents_dag_dict.keys():
                self.parents_dag_dict[stage_id] = stage_parents
            else:
                self.parents_dag_dict[stage_id] += stage_parents

    # Fix the DAG after parsing such that the parent stages reflect actual stages run in the job
    def decipher_dag(self):
        # Iterate over the child stages
        stages = self.parents_dag_dict.keys()
        for child in stages:
            # Iterate over each child stage's parent stage(s)
            parents = self.parents_dag_dict[child]
            for parent in range(len(parents)):
                stage_id = self.parents_dag_dict[child][parent]

                # If this parent stage_id wasn't run, find its alias
                if stage_id not in stages:
                    trans_id = self.stage_dict[stage_id]
                    alias    = [x for x in self.alias_dict[trans_id] if x in stages][0]
                    self.parents_dag_dict[child][parent] = alias

            # If parents are an empty array, make sure there are no inter-job dependencies via persisted RDDs
            # NOTE: This only happens between jobs
            if not self.parents_dag_dict[child]:
                rdd_ids = self.stage_rdd_dict[child]
                if self.debug: 
                    print("Stage " + str(child) + " has no parents... Parsing...")
                    print("RDDs in stage: " + str(rdd_ids))

                # Grab all potential parent stages (i.e., those that report to run this stage's RDD parents)
                parents = {}
                for rdd_id in rdd_ids:
                    # Remove duplicate stages (i.e., due to Spark internal DAG data structures)
                    rdd_parents       = list(set(self.rdd_parent_dict[rdd_id]))                       # <-- The parent RDD(s) of transformation rdd_id in stage[child]
                    potential_parents = []                                                            # <-- Place to accumulate all parent stages that meet criteria
                    for parent in rdd_parents:
                        # Filter first for the stages whose stage_id is less than this stage (i.e., came before)
                        parent_stages = [x for x in self.rdd_stage_dict[parent] if x < child]         # <-- Criteria #1: the stages fetched have to have run before the current stage

                        # Filter second for the stages that actually chose to persist this RDD
                        parent_stages = [x for x in parent_stages                                      
                                        if rdd_id in self.rdd_persist_dict[x].keys() and              # <-- Criteria #2: the stages fetched have to be in the persist dictionary
                                        self.rdd_persist_dict[x][rdd_id]]                             # <-- Criteria #3: the stages fetched have to have persisted the requested RDD

                        # Add all such stages to the list of potential parents
                        potential_parents += parent_stages

                    # Record this potential parent stage
                    for parent in potential_parents:
                        parents[parent] = None  
    
                # Identify the minimum potential parent that was actually run by the application
                #  - The basic idea is that, if persisted, this stage is dependent upon the earliest persist of the RDD
                parent = list(parents.keys())
                if self.debug: 
                    print("Stage: " + str(child) + " potential parents: " + str(parent))

                # Filter out stages that weren't actually run in the job
                parent = [x for x in parent if x in stages]
                if parent: parent = [min(parent)]
                self.parents_dag_dict[child] = parent

                if self.debug: 
                    print("The persist parent for the stage is: " + str(parent))
    

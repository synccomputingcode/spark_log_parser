import numpy
import logging

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
        self.result_size = task_metrics["Result Size"]/1000000 # [MB]
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

        # Locality addition
        self.locality = task_info["Locality"]

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
            self.jvm_v_memory = task_executor_metrics["ProcessTreeJVMVMemory"]
            self.jvm_rss_memory = task_executor_metrics["ProcessTreeJVMRSSMemory"]
            self.python_v_memory = task_executor_metrics["ProcessTreePythonVMemory"]
            self.python_rss_memory = task_executor_metrics["ProcessTreePythonRSSMemory"]
            self.other_v_memory = task_executor_metrics["ProcessTreeOtherVMemory"]
            self.other_rss_memory = task_executor_metrics["ProcessTreeOtherRSSMemory"]
        else:
            self.jvm_heap_memory = 0
            self.jvm_offheap_memory = 0
            self.onheap_execution_memory = 0
            self.onheap_storage_memory = 0
            self.offheap_storage_memory = 0
            self.onheap_unified_memory = 0
            self.offheap_unified_memory = 0
            self.jvm_v_memory = 0
            self.jvm_rss_memory = 0
            self.python_v_memory = 0
            self.python_rss_memory = 0
            self.other_v_memory = 0
            self.other_rss_memory = 0
            
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
        task_compute_time = self.compute_time_without_gc() - self.executor_deserialize_time 
        - self.result_serialization_time
        
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


import numpy

from .task_model import TaskModel


class StageModel:
    """
    Model for a stage within a spark job
    """

    def __init__(self):
        self.id: int = None
        self.stage_name: str = None
        # By default, Stages will have a "Stage Attempt ID" of 0 the first time they are run.
        # If a Stage fails for some reason, it may be re-tried, and appear again in the eventlog with the
        # "Stage Attempt ID" incremented by 1. As such, we want to keep track of this so that we aren't putting
        # "stale" data onto a Stage when processing the eventlog lines (i.e. we don't want to set the `submission_time`
        # to be that of Attempt ID 0 when we've already encountered Attempt ID 1!)
        self.attempt_id: int = None
        self.stage_info: dict = None
        self.start_time: int = -1
        self.tasks: list[TaskModel] = []
        self.tasks_finalized: bool = False
        self.num_tasks: int = 0
        self.submission_time: int = None
        self.completion_time: int = None

    def average_task_runtime(self):
        return (
            numpy.percentile([t.compute_time() for t in self.tasks], 50)
            if len(self.tasks) > 0
            else 0
        )

    def average_executor_deserialize_time(self):
        return (
            sum([t.executor_deserialize_time for t in self.tasks]) * 1.0 / len(self.tasks)
            if len(self.tasks) > 0
            else 0
        )

    def average_result_serialization_time(self):
        return (
            sum([t.result_serialization_time for t in self.tasks]) * 1.0 / len(self.tasks)
            if len(self.tasks) > 0
            else 0
        )

    def total_executor_deserialize_time(self):
        return sum([t.executor_deserialize_time for t in self.tasks])

    def total_result_serialization_time(self):
        return sum([t.result_serialization_time for t in self.tasks])

    def total_scheduler_delay(self):
        return sum([t.scheduler_delay for t in self.tasks])

    def total_peak_execution_memory(self):
        return sum([t.peak_execution_memory for t in self.tasks])

    def average_scheduler_delay(self):
        return (
            sum([t.scheduler_delay for t in self.tasks]) * 1.0 / len(self.tasks)
            if len(self.tasks) > 0
            else 0
        )

    def average_gc_time(self):
        return (
            sum([t.gc_time for t in self.tasks]) * 1.0 / len(self.tasks)
            if len(self.tasks) > 0
            else 0
        )

    def total_gc_time(self):
        return sum([t.gc_time for t in self.tasks])

    def has_shuffle_read(self):
        total_shuffle_read_bytes = sum(
            [t.remote_mb_read + t.local_mb_read for t in self.tasks if t.has_fetch]
        )
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

    def total_result_size(self):
        return sum([t.result_size for t in self.tasks])

    def max_disk_bytes_spilled(self):
        return max([t.disk_bytes_spilled for t in self.tasks])

    def max_memory_bytes_spilled(self):
        return max([t.memory_bytes_spilled for t in self.tasks])

    def max_task_runtime(self):
        return (
            numpy.percentile([t.compute_time() for t in self.tasks], 100)
            if len(self.tasks) > 0
            else 0
        )

    def max_peak_execution_memory(self):
        return max([t.peak_execution_memory for t in self.tasks])

    def total_runtime_no_remote_shuffle_read(self):
        return sum([t.runtime_no_remote_shuffle_read() for t in self.tasks])

    def total_time_fetching(self):
        return sum([t.total_time_fetching for t in self.tasks if t.has_fetch])

    def input_mb(self):
        """Returns the total input size for this stage.

        This is only valid if the stage read data from a shuffle.
        """
        total_input_bytes = sum(
            [t.remote_mb_read + t.local_mb_read for t in self.tasks if t.has_fetch]
        )
        total_input_bytes += sum([t.input_mb for t in self.tasks])
        return total_input_bytes

    def output_mb(self):
        """Returns the total output size for this stage.

        This is only valid if the output data was written for a shuffle.
        TODO: Add HDFS / in-memory RDD output size.
        """
        total_output_size = sum([t.shuffle_mb_written for t in self.tasks])
        return total_output_size

    def add_event(self, data, is_json):
        # TODO: account for failed tasks
        if "Task Metrics" in data:
            task = TaskModel(data, is_json)
            self.add_task(task)

    def add_task(self, task: TaskModel):
        if self.tasks_finalized:
            raise RuntimeError(
                "Attempted to add a task to a stage after that stage had already been finalized."
            )

        if self.start_time == -1:
            self.start_time = task.start_time
        else:
            self.start_time = min(self.start_time, task.start_time)

        self.tasks.append(task)

    def finalize_tasks(self):
        """
        Since tasks may be added to Stages out-of-order, this method should be called once we know we are done
        processing all Tasks.
        """
        self.tasks_finalized = True
        # When log lines are read in-order, SparkListenerTaskEnd lines will be present in the log ordered by their
        # finish_time (but, if multiple Tasks end at the same time, the order they appear in the eventlog is
        # non-deterministic). Since we receive the tasks out-of-order, we can just do a sort on them to put them
        # in their "correct" order
        self.tasks.sort(key=lambda t: t.finish_time)

    def get_features(self, stage_id):
        """
        Return features from stage to write to csv
        """
        features = []
        for t in self.tasks:
            features.append(
                [
                    stage_id,
                    t.executor_id,
                    t.start_time,
                    t.finish_time,
                    t.executor,
                    t.executor_run_time,
                    t.executor_deserialize_time,
                    t.result_serialization_time,
                    t.gc_time,
                    t.network_bytes_transmitted_ps,
                    t.network_bytes_received_ps,
                    t.process_cpu_utilization,
                    t.total_cpu_utilization,
                    t.shuffle_write_time,
                    t.shuffle_mb_written,
                    t.input_read_time,
                    t.input_mb,
                    t.output_mb,
                    t.has_fetch,
                    t.data_local,
                    t.local_mb_read,
                    t.local_read_time,
                    t.total_time_fetching,
                    t.remote_mb_read,
                    t.scheduler_delay,
                ]
            )

        return features

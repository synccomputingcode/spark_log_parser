import collections
import logging
import numpy

from .stage_model import StageModel


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
            del self.stages[id]

        # Compute the amount of overlapped time between stages
        # (there should just be two stages, at the beginning, that overlap and run concurrently).
        # This computation assumes that not more than two stages overlap.
        start_and_finish_times = [(stage_id, s.start_time, s.conservative_finish_time())
                                  for stage_id, s in self.stages.items()]
        start_and_finish_times.sort(key=lambda x: x[1])
        self.overlap = 0
        old_end = 0
        previous_id = ""
        self.stages_to_combine = set()
        for id, start, finish in start_and_finish_times:
            if start < old_end:
                self.overlap += old_end - start
                self.stages_to_combine.add(id)
                self.stages_to_combine.add(previous_id)
                old_end = max(old_end, finish)
            if finish > old_end:
                old_end = finish
                previous_id = id

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
            f.write(
                'stage_id, executor_id, start_time, finish_time, executor, executor_run_time, executor_deserialize_time, result_serialization_time, gc_time, network_bytes_transmitted_ps, network_bytes_received_ps, process_cpu_utilization, total_cpu_utilization, shuffle_write_time, shuffle_mb_written, input_read_time, input_mb, output_mb, has_fetch, data_local, local_mb_read, local_read_time, total_time_fetching, remote_mb_read, scheduler_delay\n')
            for id, stage in self.stages.items():
                numpy.savetxt(f, stage.get_features(id), delimiter=',', fmt="%s")

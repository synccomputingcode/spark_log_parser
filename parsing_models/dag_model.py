import json
import re
from collections import defaultdict


def parse_line(line, pattern, var_names):
    # General function to parse a line and put in dictionary with given list of variable names.
    parsed_dict = {}
    match = re.match(pattern, line)
    if match:
        for index, var_name in enumerate(var_names):
            parsed_dict[var_name] = match.group(index + 1)

    return parsed_dict


# Extracts Spark DAG information (i.e., the stage-level & RDD-level dependency structure)
class DagModel:
    def __init__(self):
        self.stage_dict = {}  # <k="stage_id", v="trans_id" >
        self.alias_dict = defaultdict(lambda: [])  # <k="trans_id", v=["stage_id"]>
        self.parents_dag_dict = defaultdict(lambda: [])  # <k="parent_stage", v=["child_stage"]>
        self.rdd_stage_dict = defaultdict(lambda: [])  # <k="RDD_id", v=["Stage_IDs"]  >
        self.stage_rdd_dict = defaultdict(lambda: [])  # <k="stage_id", v=["RDD_ids"]    >
        self.rdd_parent_dict = defaultdict(lambda: [])  # <k="RDD_id", v=["RDD_parents"]>
        self.rdd_persist_dict = {}  # <k="stage_id", v="persist?"     >
        self.broadcast_stages = []  # stages that have BroadcastExchange

    # Extract the DAG from the Spark log SparkListenerJobStart and SparkListenerStageStart
    def parse_dag(self, data):
        # Create the stage alias dictionaries
        # NOTE: SparkListenerJobStart will always be before SparkListenerStageStart
        if "Stage Infos" in data.keys():
            stages = data["Stage Infos"]
            for stage in stages:

                # Prepare stage-level data
                stage_id = stage["Stage ID"]
                stage_parents = stage["Parent IDs"]
                rdd_dict = set({})

                # Only initialize the persist dictionary on the first instance of a stage
                if stage_id not in self.rdd_persist_dict.keys():
                    self.rdd_persist_dict[stage_id] = {}

                # Iterate through all RDDs in a stage
                for x in stage["RDD Info"]:

                    # Extract the given RDD's ID from the stage data and its parent RDDs
                    rdd_id = x["RDD ID"]
                    rdd_parents = x["Parent IDs"]

                    # Keep track of whether this RDD was persisted
                    use_disk = bool(x["Storage Level"]["Use Disk"])
                    use_memory = bool(x["Storage Level"]["Use Memory"])
                    persisted = use_disk or use_memory
                    if rdd_id not in self.rdd_persist_dict[stage_id]:
                        self.rdd_persist_dict[stage_id][rdd_id] = persisted

                    # If there is a duplicate instance of this RDD, consider it cached if any version specifies caching
                    else:
                        previous_persist = self.rdd_persist_dict[stage_id][rdd_id]
                        self.rdd_persist_dict[stage_id][rdd_id] = persisted or previous_persist

                    # Keep track of RDDs for this stage -- the keys provide a branchless set
                    rdd_dict.add(rdd_id)

                    self.rdd_parent_dict[rdd_id] += rdd_parents  # Keep track of RDD-level parents
                    self.stage_rdd_dict[stage_id].append(
                        rdd_id
                    )  # Keep track of which RDD(s) belong to this stage
                    self.rdd_stage_dict[rdd_id].append(
                        stage_id
                    )  # Keep track of which Stage(s) an RDD belongs to

                trans_id = str(rdd_dict)  # Create an identifier for this stage and store aliases
                self.alias_dict[trans_id].append(stage_id)
                self.stage_dict[
                    stage_id
                ] = trans_id  # Save the dictionary of unique stage identifiers

        # Stages will be processed in execution order.
        if "Stage Info" in data.keys():
            stage_info = data["Stage Info"]
            stage_id = stage_info["Stage ID"]
            stage_parents = stage_info["Parent IDs"]

            rdd_scope = {}
            # get stages where broadcast exchange occurs
            try:
                rdd_scope = json.loads(stage_info["RDD Info"][0]["Scope"])
            except KeyError:
                rdd_scope["name"] = None

            # keep track of which stages have broadcasts
            if rdd_scope["name"] == "BroadcastExchange":
                self.broadcast_stages.append(stage_id)

            # Setup parents
            self.parents_dag_dict[stage_id] += stage_parents

    # Fix the DAG after parsing such that the parent stages reflect actual stages run in the job
    def decipher_dag(self):

        # phantom_dependencies are stages which appear as parents of other stages, but don't
        # themselves other run. So far, this has only been observed when a stage has 0 tasks
        # and is therefore "skipped", but still remains as a dependency to another stage.
        phantom_dependencies = defaultdict(lambda: [])

        # Iterate over the child stages
        stages = self.parents_dag_dict.keys()
        for child in stages:
            # Iterate over each child stage's parent stage(s)
            parents = self.parents_dag_dict[child]
            for parent in range(len(parents)):
                stage_id = self.parents_dag_dict[child][parent]

                # If this parent stage_id wasn't run, find its alias
                # Only alias if both a) the stage wasn't run and b) an alias exists
                if stage_id not in stages:
                    trans_id = self.stage_dict[stage_id]
                    alias = [x for x in self.alias_dict[trans_id] if x in stages]
                    if alias:
                        self.parents_dag_dict[child][parent] = alias[0]
                    else:
                        parent_id = parents[parent]
                        phantom_dependencies[child].append(parent_id)

            # If parents are an empty array, make sure there are no inter-job dependencies via persisted RDDs
            # NOTE: This only happens between jobs
            if not self.parents_dag_dict[child]:
                self.check_persisted_rdd_dependencies(child)

        # Remove phantom dependencies after the loop
        for child in phantom_dependencies.keys():
            phantoms = phantom_dependencies[child]
            for phantom in phantoms:
                self.parents_dag_dict[child].remove(phantom)

    def check_persisted_rdd_dependencies(self, child):
        stages = self.parents_dag_dict.keys()
        potential_parents = []
        # Grab all potential parent stages (i.e., those that report to run this stage's RDD parents)
        for rdd_id in self.stage_rdd_dict[child]:
            # Remove duplicate stages (i.e., due to Spark internal DAG data structures)
            for parent in set(self.rdd_parent_dict[rdd_id]):
                # Filter first for the stages whose stage_id is less than this stage (i.e., came before)
                # Criteria #1: the stages fetched have to have run before the current stage
                parent_stages = [x for x in self.rdd_stage_dict[parent] if x < child]

                # Filter second for the stages that actually chose to persist this RDD
                # Criteria #2: the stages fetched have to be in the persist dictionary
                # Criteria #3: the stages fetched have to have persisted the requested RDD
                parent_stages = [
                    x
                    for x in parent_stages
                    if rdd_id in self.rdd_persist_dict[x] and self.rdd_persist_dict[x][rdd_id]
                ]

                # Add all such stages to the list of potential parents
                potential_parents += parent_stages

        # Identify the minimum potential parent that was actually run by the application
        #  - The basic idea is that, if persisted, this stage is dependent upon the earliest persist of the RDD
        # Filter out stages that weren't actually run in the job -- it is possible for a stage to have no parents
        parent = [x for x in set(potential_parents) if x in stages]
        if parent:
            parent = [min(parent)]
        self.parents_dag_dict[child] = parent

    def add_broadcast_dependencies(self, stdoutpath):
        # add implicit dependencies when there is a broadcast variable created
        if stdoutpath is not None:
            self.parse_stdout(stdoutpath)

        # add implicit dependencies from both broadcast variables (if stdout provided) and BroadcastExchange
        stage_list = list(self.parents_dag_dict.keys())
        for parent_stage in reversed(self.broadcast_stages):
            idx = stage_list.index(parent_stage) + 1
            for stage_id in stage_list[idx:]:
                # add parent stage if it is not already there, and if it doesn't have dependencies on later stages already
                # keeping logic that does not assume sequential stage execution to be able to parse logs we run with our forked Spark
                if (parent_stage not in self.parents_dag_dict[stage_id]) and (
                    not any(s in stage_list[idx:] for s in self.parents_dag_dict[stage_id])
                ):
                    self.parents_dag_dict[stage_id].append(parent_stage)

    def parse_broadcast(self, lines):
        # Parse lines for broadcast (other than from DAGScheduler) and add implicit dependencies
        # patterns to parse
        broadcast_pattern = ".*?SparkContext: Created broadcast [0-9]* from .*? at (.*?):[0-9]*"
        stage_pattern = ".*?Killing all running tasks in stage ([0-9]*): Stage finished"

        # variable names corresponding to patterns
        broadcast_vars = ["source"]
        stage_vars = ["stage"]

        stage_list = list(self.parents_dag_dict.keys())
        parent_stage = stage_list[0]

        for line in lines:
            parsed_stage = parse_line(line, stage_pattern, stage_vars)
            # keep track of which stage is finished most recently
            # assumes broadcast is dependent on the last stage completing
            # this MAY NOT BE TRUE, we'll really only know when we test new schedules
            if parsed_stage:
                parent_stage = int(parsed_stage["stage"])

            parsed_broadcast = parse_line(line, broadcast_pattern, broadcast_vars)
            # only add dependencies is broadcast is not from DAGScheduler
            cond_1 = parent_stage not in self.broadcast_stages
            cond_2 = parsed_broadcast["source"] != "DAGScheduler.scala"
            if parsed_broadcast and cond_1 and cond_2:
                self.broadcast_stages.append(parent_stage)

    def parse_stdout(self, stdoutpath):
        with open(stdoutpath) as f:
            lines = f.read().splitlines()
            self.parse_broadcast(lines)

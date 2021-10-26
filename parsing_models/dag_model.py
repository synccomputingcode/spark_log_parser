import re
import json


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
        self.debug            = False # NOTE: For development purposes
        self.stage_dict       = {}    # <key="stage_id",     value="trans_id"     >
        self.alias_dict       = {}    # <key="trans_id",     value=["stage_id"]   >
        self.parents_dag_dict = {}    # <key="parent_stage", value=["child_stage"]>
        self.rdd_stage_dict   = {}    # <key="RDD_id",       value=["Stage_IDs"]  >
        self.stage_rdd_dict   = {}    # <key="stage_id",     value=["RDD_ids"]    >
        self.rdd_parent_dict  = {}    # <key="RDD_id",       value=["RDD_parents"]>
        self.rdd_persist_dict = {}    # <key="stage_id",     value="persist?"     >
        self.broadcast_stages = []    # stages that have BroadcastExchange

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

                # Prepare stage-level data
                stage_id                        = stage["Stage ID"]
                stage_parents                   = stage["Parent IDs"]
                rdd_dict                        = {}

                # Only initialize the persist dictionary on the first instance of a stage
                if stage_id not in self.rdd_persist_dict.keys():
                    self.rdd_persist_dict[stage_id] = {}

                # Iterate through all RDDs in a stage
                for x in stage["RDD Info"]:
                    if self.debug: 
                        print("\tRDD " + str(x["RDD ID"])            + \
                              " has parents " + str(x["Parent IDs"]) + \
                              " and is named " + str(x["Scope"]))

                    # Extract the given RDD's ID from the stage data and its parent RDDs
                    rdd_id      = x["RDD ID"]
                    rdd_parents = x["Parent IDs"]

                    # Keep track of whether this RDD was persisted
                    use_disk   = bool(x["Storage Level"]["Use Disk"])
                    use_memory = bool(x["Storage Level"]["Use Memory"])
                    persisted  = use_disk or use_memory
                    if rdd_id not in self.rdd_persist_dict[stage_id].keys():
                        self.rdd_persist_dict[stage_id][rdd_id] = persisted

                    # If there is a duplicate instance of this RDD, consider it cached if any version specifies caching
                    else:
                        previous_persist                        = self.rdd_persist_dict[stage_id][rdd_id]
                        self.rdd_persist_dict[stage_id][rdd_id] = persisted or previous_persist


                    # Keep track of RDDs for this stage -- the keys provide a branchless set
                    rdd_dict[rdd_id] = None

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

            if self.debug: 
                print("Stage " + str(stage_id) + " has parents " + str(stage_parents))

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
            if stage_id not in self.parents_dag_dict.keys():
                self.parents_dag_dict[stage_id] = stage_parents
            else:
                self.parents_dag_dict[stage_id] += stage_parents

    # Fix the DAG after parsing such that the parent stages reflect actual stages run in the job
    def decipher_dag(self):
        # Create a dictionary of dependencies to remove (e.g., avoids corrupting the loop below)
        # this only occurs when a stage, reported as a parent stage, neither runs nor is aliased
        phantom_dependencies = {}

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
                    alias    = [x for x in self.alias_dict[trans_id] if x in stages]
                    if alias:
                        self.parents_dag_dict[child][parent] = alias[0]
                    else:
                        print("Warning! Stage " + str(child) + " parent " + str(parent) + " has no alias, and was not run!")

                        # Track dependency to remove after the loop
                        if child not in phantom_dependencies.keys():
                            phantom_dependencies[child] = [parent]
                        else:
                            phantom_dependencies[child].append(parent)

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

                    # Record this potential parent stage -- the keys provide a branchless set
                    for parent in potential_parents: 
                        parents[parent] = None  
    
                # Identify the minimum potential parent that was actually run by the application
                #  - The basic idea is that, if persisted, this stage is dependent upon the earliest persist of the RDD
                parent = list(parents.keys())
                if self.debug: 
                    print("Stage: " + str(child) + " potential parents: " + str(parent))

                # Filter out stages that weren't actually run in the job -- it is possible for a stage to have no parents
                parent = [x for x in parent if x in stages]
                if parent: parent = [min(parent)]
                self.parents_dag_dict[child] = parent
                if self.debug: 
                    print("The persist parent for the stage is: " + str(parent))
    
        # Remove phantom dependencies after the loop
        for child in phantom_dependencies.keys():
            phantoms = phantom_dependencies[child]
            for phantom in phantoms:
                self.parents_dag_dict[child].remove(phantom)

    def add_broadcast_dependencies(self, stdoutpath):
        # add implicit dependencies when there is a broadcast variable created
        if stdoutpath is not None:
            self.parse_stdout(stdoutpath)

        # add implicit dependencies from both broadcast variables (if stdout provided) and BroadcastExchange
        stage_list = list(self.parents_dag_dict.keys())
        for parent_stage in reversed(self.broadcast_stages):
            for stage_id in stage_list[stage_list.index(parent_stage)+1:]:
                # add parent stage if it is not already there, and if it doesn't have dependencies on later stages already
                # keeping logic that does not assume sequential stage execution to be able to parse logs we run with our forked Spark
                if (parent_stage not in self.parents_dag_dict[stage_id]) and (not any(s in stage_list[stage_list.index(parent_stage)+1:] for s in self.parents_dag_dict[stage_id])):
                    self.parents_dag_dict[stage_id].append(
                        parent_stage)

    def parse_broadcast(self, lines):
        # Parse lines for broadcast (other than from DAGScheduler) and add implicit dependencies
        # patterns to parse
        broadcast_pattern = '.*?SparkContext: Created broadcast [0-9]* from .*? at (.*?):[0-9]*'
        stage_pattern = '.*?Killing all running tasks in stage ([0-9]*): Stage finished'

        # variable names corresponding to patterns
        broadcast_vars = ['source']
        stage_vars = ['stage']
        
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
            if (parsed_broadcast and (parsed_broadcast["source"] != "DAGScheduler.scala" and parent_stage not in self.broadcast_stages)):
                self.broadcast_stages.append(parent_stage)

    def parse_stdout(self, stdoutpath):
        with open(stdoutpath) as f:
            lines = f.read().splitlines()
            self.parse_broadcast(lines)

import numpy


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
        
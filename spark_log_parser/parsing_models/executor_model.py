class ExecutorModel:
    """
    Model for a Spark Executor (i.e. worker node)
    """

    id: str = None
    host: str = None
    cores: int = None
    start_time: int = None
    end_time: int = None
    removed_reason: str = ""

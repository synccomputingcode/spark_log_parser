import logging

from .exceptions import LazyEventValidationException
from .exceptions import ParserErrorMessages as MSGS

logger = logging.getLogger("EventDataValidation")


class EventDataValidation:
    """
    Validate the existence of certain Spark Listener Events
    """

    def __init__(self, app=None, debug=False):

        self.app = app
        self.debug = debug  # When 'True' disables exception raises for debugging
        self.message = ""

    def validate(self):
        """
        Run the validation methods. If one or more errors exist then log the error, then throw
        and exception. Logging is used here so that the problem is still indicated when in debug
        mode.
        """
        if not self.app.finish_time:
            self.message += (
                MSGS.MISSING_EVENT_GENERIC_MESSAGE + "'Application / Stage / SQL Completion'. "
            )

        self.validate_job_events()
        self.validate_stage_events()

        if len(self.message) > 0:
            logger.error(self.message)
            if not self.debug:
                raise LazyEventValidationException(error_message=self.message)

    def validate_job_events(self):
        """
        Look for missing job events.

        4/20/2022: Currently only the JobComplete is detected, because a missing
        JobStart event will result in a more urgent exception being thrown in SparkApplication
        """

        miss_job_ends = []
        for jid, job in self.app.jobs.items():
            if not hasattr(job, "completion_time"):
                miss_job_ends.append(jid)

        if len(miss_job_ends) > 0:
            self.message += f"{MSGS.MISSING_EVENT_JOB_END}{miss_job_ends}. "

    def validate_stage_events(self):
        """
        Look for missing stage events.

        4/20/2022: Currently only the StageSubmitted is detected, because a
        missing StageCompleted event will result in a more urgent exception being thrown in
        SparkApplication
        """
        miss_stage_completes = []
        for jid, job in self.app.jobs.items():
            for sid, stage in job.stages.items():
                if not hasattr(stage, "completion_time"):
                    miss_stage_completes.append(sid)

        if len(miss_stage_completes) > 0:
            self.message += f"{MSGS.MISSING_EVENT_STAGE_COMPLETE}{miss_stage_completes}.  "

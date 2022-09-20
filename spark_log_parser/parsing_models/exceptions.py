import json
import logging

from .errors import ParserErrorCodes, ParserErrorMessages, ParserErrorTypes

logger = logging.getLogger("ParserExceptionLogger")


class SyncParserException(Exception):
    def __init__(
        self,
        error_type: str = None,
        error_message: str = None,
        status_code: int = None,
        exception: Exception = None,
    ):

        super().__init__(error_message)

        self.error_type = error_type
        self.error_message = error_message
        self.status_code = status_code

        # 2022-03-25 RW:  Format the information in the way it is expected by the backend code
        self.error = {"error": error_type, "message": error_message}

        logger.error(error_message)
        if exception:
            logger.exception(exception)

    def get_ui_return_value(self) -> dict:
        """
        A possible rendering of one set of return information as dict
        """
        return {"error": self.error_type, "message": self.error_message}

    def get_ui_return_value_as_json(self) -> json:
        """
        A possible rendering of one set of return information as JSON
        """
        return json.dumps({"error": self.error_type, "message": self.error_message})

    def get_exception_response(self) -> dict:
        """
        Used to get an example response, which will be used for
        the swagger documentation.

        Returns:
            dict: error response
        """
        # Convert into a pydantic model in future iterations
        # once flask is stripped away
        return {
            "error": {
                "type": self.error_type,
                "code": self.status_code,
                "message": self.error_message,
            }
        }


class ConfigurationException(SyncParserException):
    def __init__(self, config_recs: str):

        error_message = ParserErrorMessages.SPARK_CONFIG_GENERIC_MESSAGE

        for idx, c in enumerate(config_recs):
            count = idx + 1
            error_message += f"  ({count}) {c}"

        error_message += f". {ParserErrorMessages.SUPPORT_MESSAGE}"

        super().__init__(
            error_type=ParserErrorTypes.SPARK_CONFIG_ERROR,
            error_message=error_message,
            status_code=ParserErrorCodes.SPARK_CONFIG_ERROR,
            exception=None,
        )


class LazyEventValidationException(SyncParserException):
    """
    This Exception is for missing event data that doesn't immediately kill the parser.
    All of the related missing events can be gathered and identified in the error message.
    """

    def __init__(self, error_message: str):

        error_message += (
            f"{ParserErrorMessages.MISSING_EVENT_EXPLANATION} "
            + f"{ParserErrorMessages.SUPPORT_MESSAGE}"
        )

        super().__init__(
            error_type=ParserErrorTypes.MISSING_EVENT_ERROR,
            error_message=error_message,
            status_code=ParserErrorCodes.SPARK_EVENT_ERROR,
            exception=None,
        )


class UrgentEventValidationException(SyncParserException):
    """
    This Exception is for missing event data that stops the parser dead in its tracks.
    """

    def __init__(self, missing_event: str = ""):

        error_message = (
            f"{ParserErrorMessages.MISSING_EVENT_GENERIC_MESSAGE} '{missing_event}'. "
            + f"{ParserErrorMessages.MISSING_EVENT_EXPLANATION} "
            + f"{ParserErrorMessages.SUPPORT_MESSAGE}"
        )

        super().__init__(
            error_type=ParserErrorTypes.MISSING_EVENT_ERROR,
            error_message=error_message,
            status_code=ParserErrorCodes.SPARK_EVENT_ERROR,
            exception=None,
        )


class LogSubmissionException(SyncParserException):
    """
    This Exception is for malformed log submission
    """

    def __init__(self, error_message: str):

        super().__init__(
            error_type=ParserErrorTypes.LOG_SUBMISSION_ERROR,
            error_message=error_message,
            status_code=ParserErrorCodes.LOG_SUBMISSION_ERROR,
            exception=None,
        )

import tempfile
import tracemalloc
from pathlib import Path

from memory_profiler import profile

from spark_log_parser.eventlog import EventLogBuilder
from spark_log_parser.extractor import Extractor, ExtractThresholds
from spark_log_parser.parsing_models.application_model_v2 import create_spark_application


# @profile
def parse_app_streaming():
    # spark_eventlog_path = Path(str(Path.home()), "Downloads", "application_1633462029662_0002").resolve()
    spark_eventlog_path = Path(str(Path.home()), "Downloads", "application_1660309901228_0002").resolve()
    spark_app = create_spark_application(spark_eventlog_path=str(spark_eventlog_path))

# for idx in range(10):
#     parse_app_streaming()
#     print(f"idx {idx} done")

parse_app_streaming()
# tracemalloc.start()
# parse_app_streaming()
# snapshot = tracemalloc.take_snapshot()
#
# print("[ Top 100 ]")
# for stat in snapshot.statistics("lineno")[:100]:
#     print(stat)
# print(snapshot)
# parse_app_local_file()

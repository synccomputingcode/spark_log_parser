from pathlib import Path

import pytest

from spark_log_parser.parsing_models.application_model_v2 import SparkApplication
from tests import zip_to_tgz


@pytest.fixture
def parsed_files(request) -> list[dict | SparkApplication]:
    """
    Given a reference to some .zip file and some parsing function, this fixture will -
    - Parse the .zip archive using the given `parse_fn`,
    - Convert the .zip archive to other archive formats we support (right now - .tgz / .tar.gz) and parse those,
        - Remove any files written to disk,
    - Return a list of all the parsed files
    """
    (zip_path, parse_fn) = request.param
    parsed_zip = parse_fn(zip_path)

    tgz_archives = []
    for ext in [".tgz", ".tar.gz"]:
        try:
            tgz = zip_to_tgz(zip_path, ext)
            filepath = Path(tgz.name)
            parsed = parse_fn(filepath)
            tgz_archives.append(parsed)
        finally:
            # We have the parsed, in-memory representation - that means we can remove the file we generated on disk
            #  and return only the parsed app to the test. We put this in a `finally` block so that the temp file
            #  on disk is always removed
            filepath.unlink(missing_ok=True)

    return [parsed_zip, *tgz_archives]

import os
import tarfile
import tempfile
import zipfile
from pathlib import Path

import pytest

from spark_log_parser.parsing_models.application_model_v2 import SparkApplication


def zip_to_tgz(zip_path: str | Path, file_suffix: str):
    """
    Given a zip archive residing at the provided Path, converts to a .tgz archive
    """
    zip_path = zip_path if isinstance(zip_path, Path) else Path(zip_path)
    with tempfile.TemporaryDirectory() as temp_dir:
        with zipfile.ZipFile(zip_path) as zfile:
            zfile.extractall(temp_dir)
            # Grab the last part of the original path to use as the filename for the new one
            tgz_filename = zip_path.parts[-1]
            # Remove suffixes from the original file name in reverse order
            tgz_filename = tgz_filename.removesuffix("".join(zip_path.suffixes))

            tgz_filename += file_suffix
            with tarfile.open(tgz_filename, "w:gz") as tarball:
                tarball.add(temp_dir, arcname=os.path.sep)
                return tarball
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
    for ext in [".tgz"]:
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
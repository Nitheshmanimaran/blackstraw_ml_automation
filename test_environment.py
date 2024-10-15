import sys

def test_python_version():
    REQUIRED_PYTHON = "python3"
    system_major = sys.version_info.major
    required_major = 3 if REQUIRED_PYTHON == "python3" else 2
    assert system_major == required_major, f"Python {required_major} is required, found Python {system_major}"
"""Added to implement solution from https://stackoverflow.com/questions/40880259/how-to-pass-arguments-in-pytest-by-command-line"""


def pytest_addoption(parser):
    parser.addoption(
        "--all_data",
        action="store_true",
        help="Include all data. If false limited_PUMA arg is set to false in load_PUMS",
    )


def pytest_generate_tests(metafunc):
    # This is called for every test. Only get/set command line arguments
    # if the argument is specified in the list of test "fixturenames".
    option_value = metafunc.config.option.all_data
    if "all_data" in metafunc.fixturenames and option_value is not None:
        metafunc.parametrize("all_data", [option_value])

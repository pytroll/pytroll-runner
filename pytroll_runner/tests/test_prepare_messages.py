"""Tests for log output processing."""

import pytest

from pytroll_runner import get_newfiles_from_regex_and_logoutput, read_config

TEST_YAML_CONFIG1 = """script: /bin/awsat_l0l1b_run.sh
publisher_config:
  output_files_log_regex: "renamed .* -> '(.*.nc)"
  publisher_settings:
    nameserver: 'localhost'
    name: 'aws-dr-runner'
    port: 0
  static_metadata:
    sensor: mwr
    format: l1b
    product: aws_level1b
  topic: /awsat/kangerlussuaq/1B/netcdf
subscriber_config:
  # addresses: []
  topics:
    - /awsat/raw/dmi/kangerlussuaq
  nameserver: 'localhost'
  addr_listener: true
"""

EXAMPLE_LOG_OUTPUT_BYTES = (b"""\nGenerating configuration..\n\nSetting SELinux context on...\n\nFound result file:"""
                            b"""\n\nrenamed '/san1/polar_in/direct_readout/aws/L1/W_XX-SMHI-Kangerlussuaq,SAT,AWS1-"""
                            b"""MWR-1B-RAD_C_SMHI_20250602201120_L_D_20250602033328_20250602033625_C_N____.nc' -> '"""
                            b"""/san1/polar_in/direct_readout/aws/lvl1/W_XX-SMHI-Kangerlussuaq,SAT,AWS1-MWR-1B-RAD_"""
                            b"""C_SMHI_20250602201120_L_D_20250602033328_20250602033625_C_N____.nc'\n""")

EXPECTED_LIST = ["/san1/polar_in/direct_readout/aws/lvl1/W_XX-SMHI-Kangerlussuaq,SAT,AWS1-MWR-1B-RAD_C_SMHI_20250602201"
                 "120_L_D_20250602033328_20250602033625_C_N____.nc"]

@pytest.fixture
def fake_config_yaml_file1(tmp_path):
    """Write fake config yaml file."""
    file_path = tmp_path / "some_config_file.yaml"
    with open(file_path, "w") as fpt:
        fpt.write(TEST_YAML_CONFIG1)

    return file_path


def test_get_newfiles_from_regex_and_logoutput(fake_config_yaml_file1):
    """Test getting new files from regex pattern and log output."""
    log_output = EXAMPLE_LOG_OUTPUT_BYTES
    config = read_config(fake_config_yaml_file1)

    pattern = config[2]["output_files_log_regex"]
    result = get_newfiles_from_regex_and_logoutput(pattern, log_output)
    assert result == EXPECTED_LIST

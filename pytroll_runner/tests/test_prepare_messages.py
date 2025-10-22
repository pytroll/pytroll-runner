"""Tests for log output processing."""

import pytest

from pytroll_runner import get_newfiles_from_regex_and_logoutput, read_config

TEST_YAML_CONFIG_ONE_OUTPUT_FILE = """script: /bin/awsat_l0l1b_run.sh
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

TEST_YAML_CONFIG_MANY_OUTPUT_FILES = """
script:
  command: "/san1/opt/pps_mw_aws_runner/releases/pps_mw_aws_runner-0.0.10/bin/pps_mw.sh -p pr_hl -s aws -r W_.*.nc "
  workers: 1
publisher_config:
  output_files_log_regex:
    - "Has written level2 file: (.*.nc)"
    - "Has saved plot file: (.*euro4.png)"
    - "Has saved plot file: (.*baltrad4.png)"
  publisher_settings:
    name: pps_mw_aws_runner
  static_metadata:
    data_processing_level: 2
    type: NC
  topic: /PPS-MW-NC/2/
subscriber_config:
  topics:
    - /awsat/l1b/metno/oslo
    - /awsat/l1b/fmi/sodankyla
  nameserver: localhost
  addr_listener: true
"""

EXAMPLE_LOG_OUTPUT_BYTES = (b"""\nGenerating configuration..\n\nSetting SELinux context on...\n\nFound result file:"""
                            b"""\n\nrenamed '/san1/polar_in/direct_readout/aws/L1/W_XX-SMHI-Kangerlussuaq,SAT,AWS1-"""
                            b"""MWR-1B-RAD_C_SMHI_20250602201120_L_D_20250602033328_20250602033625_C_N____.nc' -> '"""
                            b"""/san1/polar_in/direct_readout/aws/lvl1/W_XX-SMHI-Kangerlussuaq,SAT,AWS1-MWR-1B-RAD_"""
                            b"""C_SMHI_20250602201120_L_D_20250602033328_20250602033625_C_N____.nc'\n""")

EXPECTED_LIST = ["/san1/polar_in/direct_readout/aws/lvl1/W_XX-SMHI-Kangerlussuaq,SAT,AWS1-MWR-1B-RAD_C_SMHI_20250602201"
                 "120_L_D_20250602033328_20250602033625_C_N____.nc"]

LOG_OUTPUT_SEVERAL_FILES = (b"""\n[INFO: 2025-10-21 11:27:47 : pps_mw.writers.level2] Start writing level2 dataset."""
                            b"""\n[INFO: 2025-10-21 11:27:47 : pps_mw.writers.level2] Has written level2 file: """
                            b"""/san1/polar_out/direct_readout/lvl2/"""
                            b"""S_NWC_PRHL_aws1_20251021T1116140Z_20251021T1123330Z.nc"""
                            b"""\n[INFO: 2025-10-21 11:27:51 : pps_mw.utils.plotting] Has saved plot file: """
                            b"""/san1/polar_out/direct_readout/lvl2/"""
                            b"""quicklook_PRHL_aws1_20251021111614_20251021112333_euro4.png"""
                            b"""\n[INFO: 2025-10-21 11:27:52 : pps_mw.utils.plotting] Has saved plot file: """
                            b"""/san1/polar_out/direct_readout/lvl2/"""
                            b"""quicklook_PRHL_aws1_20251021111614_20251021112333_baltrad4.png"""
                            b"""\n[INFO: 2025-10-21 11:27:52 : pps_mw.pges.pge_runner] Done pr_hl processing for """
                            b"""/san1/polar_in/regional/aws/l1b/W_XX-FMI-Sodankyla,SAT,AWS1-MWR-1B-RAD_C_FMI_"""
                            b"""_20251021112652_R_D_20251021111330_20251021112402_C_N____.nc.""")

EXPECTED_LIST2 = ["/san1/polar_out/direct_readout/lvl2/S_NWC_PRHL_aws1_20251021T1116140Z_20251021T1123330Z.nc",
                  "/san1/polar_out/direct_readout/lvl2/quicklook_PRHL_aws1_20251021111614_20251021112333_baltrad4.png",
                  "/san1/polar_out/direct_readout/lvl2/quicklook_PRHL_aws1_20251021111614_20251021112333_euro4.png"
                  ]

@pytest.fixture
def fake_config_yaml_one_output_file(tmp_path):
    """Write fake config yaml file."""
    file_path = tmp_path / "some_config_file.yaml"
    with open(file_path, "w") as fpt:
        fpt.write(TEST_YAML_CONFIG_ONE_OUTPUT_FILE)

    return file_path


@pytest.fixture
def fake_config_yaml_many_output_files(tmp_path):
    """Write fake config yaml file."""
    file_path = tmp_path / "some_config_many_output_files.yaml"
    with open(file_path, "w") as fpt:
        fpt.write(TEST_YAML_CONFIG_MANY_OUTPUT_FILES)

    return file_path


def test_get_newfiles_from_regex_and_logoutput(fake_config_yaml_one_output_file):
    """Test getting new files from regex pattern and log output."""
    log_output = EXAMPLE_LOG_OUTPUT_BYTES
    config = read_config(fake_config_yaml_one_output_file)

    pattern = config[2]["output_files_log_regex"]
    result = get_newfiles_from_regex_and_logoutput(pattern, log_output)
    assert result == EXPECTED_LIST


def test_get_newfiles_from_regex_patterns_and_logoutput(fake_config_yaml_many_output_files):
    """Test getting new files from regex patterns and log output."""
    log_output = LOG_OUTPUT_SEVERAL_FILES
    config = read_config(fake_config_yaml_many_output_files)

    pattern = config[2]["output_files_log_regex"]
    result = get_newfiles_from_regex_and_logoutput(pattern, log_output)

    assert sorted(result) == sorted(EXPECTED_LIST2)

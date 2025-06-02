#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2025 Pytroll developers
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""
"""

import logging
import os
import sys
from datetime import datetime
from pathlib import Path
import pytest
import yaml

from pytroll_runner import (
    get_newfiles_from_regex_and_logoutput,
    read_config)



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

EXAMPLE_LOG_OUTPUT_BYTES = """b'\nGenerating configuration..\n\nSetting SELinux context on...\n\nFound result file:\n\nrenamed \'/san1/polar_in/direct_readout/aws/L1/W_XX-SMHI-Kangerlussuaq,SAT,AWS1-MWR-1B-RAD_C_SMHI_20250602201120_L_D_20250602033328_20250602033625_C_N____.nc\' -> \'/san1/polar_in/direct_readout/aws/lvl1/W_XX-SMHI-Kangerlussuaq,SAT,AWS1-MWR-1B-RAD_C_SMHI_20250602201120_L_D_20250602033328_20250602033625_C_N____.nc\'\n'"""
EXAMPLE_LOG_OUTPUT_STRING = """\nGenerating configuration..\n\nSetting SELinux context on...\n\nFound result file:\n\nrenamed \'/san1/polar_in/direct_readout/aws/L1/W_XX-SMHI-Kangerlussuaq,SAT,AWS1-MWR-1B-RAD_C_SMHI_20250602201120_L_D_20250602033328_20250602033625_C_N____.nc\' -> \'/san1/polar_in/direct_readout/aws/lvl1/W_XX-SMHI-Kangerlussuaq,SAT,AWS1-MWR-1B-RAD_C_SMHI_20250602201120_L_D_20250602033328_20250602033625_C_N____.nc\'"""


EXPECTED_LIST = ['/san1/polar_in/direct_readout/aws/lvl1/W_XX-SMHI-Kangerlussuaq,SAT,AWS1-MWR-1B-RAD_C_SMHI_20250602201120_L_D_20250602033328_20250602033625_C_N____.nc']

@pytest.fixture
def fake_config_yaml_file1(tmp_path):
    """Write fake config yaml file."""
    file_path = tmp_path / 'some_config_file.yaml'
    with open(file_path, 'w') as fpt:
        fpt.write(TEST_YAML_CONFIG1)

    yield file_path


@pytest.mark.parametrize("log_output, expected_files",
                         [(EXAMPLE_LOG_OUTPUT_BYTES,
                           EXPECTED_LIST),
                          (EXAMPLE_LOG_OUTPUT_STRING,
                           EXPECTED_LIST),
                          ]
                         )
def test_get_newfiles_from_regex_and_logoutput(fake_config_yaml_file1, log_output, expected_files):
    """Test getting new files from regex pattern and log output."""
    config = read_config(fake_config_yaml_file1)

    pattern = config[2]['output_files_log_regex']
    result = get_newfiles_from_regex_and_logoutput(pattern, log_output)
    assert result == expected_files

"""Tests for the pytroll runner."""
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from unittest import mock

import pytest
import yaml
from posttroll.message import Message
from posttroll.testing import patched_publisher, patched_subscriber_recv

from pytroll_runner import (
    generate_message_from_expected_files,
    main,
    read_config,
    run_and_publish,
    run_from_new_subscriber,
    run_on_files,
    run_on_messages,
)


def script(redirection_specification):
    """A bash script to generate a generic output log."""
    return f"""#!/bin/bash
echo "Got $*"{redirection_specification}
"""


def script_bla(redirection_specification):
    """A bash script to generate the output log for writing files."""
    return f"""#!/bin/bash
for file in $*; do
    cp "$file" "$file.bla"
    echo "Written output file : $file.bla"{redirection_specification}
done
"""


# ruff: noqa: E501
def script_aws(redirection_specification):
    """A bash script to generate the output log for AWS."""
    return f"""#!/bin/bash
echo "2023-08-17T09:48:45.949211 fe5e1feebbfb IPF-AWS-L1 01.00 [000000000045]: [P] STEP 1: Starting IPF-AWS-L1 v1.0.1 processor (elapsed 0.000 seconds)
2023-08-17T09:48:45.949358 fe5e1feebbfb IPF-AWS-L1 01.00 [000000000045]: [P] STEP 2: Loading JobOrder (elapsed 0.000 seconds)
2023-08-17T09:48:45.950030 fe5e1feebbfb IPF-AWS-L1 01.00 [000000000045]: [P] STEP 3: Initializing AWS L1 (elapsed 0.001 seconds)
2023-08-17T09:48:45.950101 fe5e1feebbfb IPF-AWS-L1 01.00 [000000000045]: [I] Reading Configuration file : /local_disk/aws_test/conf/L1/AWS_L1_Configuration.xml
2023-08-17T09:48:46.047664 fe5e1feebbfb IPF-AWS-L1 01.00 [000000000045]: [P] STEP 4: Executing AWS L1A Module (elapsed 0.099 seconds)
2023-08-17T09:48:46.226927 fe5e1feebbfb IPF-AWS-L1 01.00 [000000000045]: [P] STEP 5: Executing AWS L1B Module (elapsed 0.278 seconds)
2023-08-17T09:48:46.321622 fe5e1feebbfb IPF-AWS-L1 01.00 [000000000045]: [P] STEP 6: Writing AWS L1 Output (elapsed 0.373 seconds)
2023-08-17T09:48:46.329884 fe5e1feebbfb IPF-AWS-L1 01.00 [000000000045]: [I] lib4eo::NcMap: Loading map file '/local_disk/aws_test/conf/L1/AWS-L1B-RAD.xsd'
2023-08-17T09:48:46.578498 fe5e1feebbfb IPF-AWS-L1 01.00 [000000000045]: [I] Written output file : /local_disk/aws_test/test/RAD_AWS_1B/W_XX-OHB-Unknown,SAT,1-AWS-1B-RAD_C_OHB_20230817094846_G_D_20220621090100_20220621090618_T_B____.nc
2023-08-17T09:48:46.588984 fe5e1feebbfb IPF-AWS-L1 01.00 [000000000045]: [P] STEP 7: Exiting (elapsed 0.640 seconds)
2023-08-17T09:48:46.589031 fe5e1feebbfb IPF-AWS-L1 01.00 [000000000045]: [I] IPF-AWS-L1 v1.0.1 processor ending with success
2023-08-17T09:48:46.589041 fe5e1feebbfb IPF-AWS-L1 01.00 [000000000045]: [I] Exiting with EXIT CODE 0"{redirection_specification}
"""


def log_config_content(output_stream=sys.stdout):
    """Configuration of the logger."""
    return f"""version: 1
disable_existing_loggers: false
handlers:
  console:
    class: logging.StreamHandler
    level: DEBUG
    stream: ext://sys.{'stderr' if output_stream is sys.stderr else 'stdout'}
root:
  level: DEBUG
  handlers: [console]
"""


@pytest.fixture(params=[sys.stdout, sys.stderr])
def output_stream(request):
    """A parametrized fixture to specify what outputstream should be used."""
    return request.param


@pytest.fixture
def redirection_specification(output_stream):
    """A fixture to redirect the output to the given stream."""
    return " >&2" if output_stream is sys.stderr else ""


@pytest.fixture
def log_config_file(output_stream, tmp_path):
    """Write a log config file."""
    log_config = tmp_path / "mylogconfig.yaml"
    with open(log_config, "w") as fobj:
        fobj.write(log_config_content(output_stream))

    return log_config


@pytest.fixture
def command(redirection_specification, tmp_path: Path) -> Path:
    """Make a command script that just prints out the files it got."""
    command_file = tmp_path / "myscript.sh"
    with open(command_file, "w") as fobj:
        _ = fobj.write(script(redirection_specification))
    os.chmod(command_file, 0o700)
    return command_file


@pytest.fixture
def command_bla(redirection_specification, tmp_path):
    """Make a command script that adds ".bla" to the filename."""
    command_file = tmp_path / "myscript_bla.sh"
    with open(command_file, "w") as fobj:
        fobj.write(script_bla(redirection_specification))
    os.chmod(command_file, 0o700)
    return command_file


@pytest.fixture
def config_bla(tmp_path, command_bla):
    """Make a config."""
    sub_config = dict(nameserver=False, addresses=["ipc://bla"])
    pub_config = dict(publisher_settings=dict(nameservers=False, port=1979),
                      expected_files=os.fspath(tmp_path / "file?.bla"),
                      topic="/hi/there")
    command_path = os.fspath(command_bla)
    test_config = dict(subscriber_config=sub_config,
                       script=command_path,
                       publisher_config=pub_config)

    return test_config


@pytest.fixture
def config_file_bla(tmp_path, config_bla):
    """Make a configuration file."""
    return write_config_file(tmp_path, config_bla)


@pytest.fixture
def command_aws(redirection_specification, tmp_path):
    """Make a command script that outputs a log with an output filename."""
    command_file = tmp_path / "myscript_aws.sh"
    with open(command_file, "w") as fobj:
        fobj.write(script_aws(redirection_specification))
    os.chmod(command_file, 0o700)
    return command_file


@pytest.fixture
def config_aws(command_aws):
    """Configuration to run aws script."""
    sub_config = dict(nameserver=False, addresses=["ipc://bla"])
    pub_config = dict(publisher_settings=dict(nameservers=False, port=1979),
                      topic="/hi/there",
                      output_files_log_regex="Written output file : (.*.nc)")
    command_path = os.fspath(command_aws)
    test_config = dict(subscriber_config=sub_config,
                       script=command_path,
                       publisher_config=pub_config)

    return test_config


@pytest.fixture
def config_file_aws(tmp_path, config_aws):
    """Make a configuration file."""
    return write_config_file(tmp_path, config_aws)


def write_config_file(tmp_path, config):
    """Write a configturation file."""
    yaml_file = tmp_path / "config.yaml"
    with open(yaml_file, "w") as fd:
        fd.write(yaml.dump(config))
    return yaml_file


def test_run_on_files_passes_files_to_script(command: Path):
    """Test that the script is called."""
    some_files = ["file1", "file2", "file3"]
    out = run_on_files(os.fspath(command), some_files)
    assert out
    assert out.decode().strip() == "Got " + " ".join(some_files)


def test_run_on_files_accepts_scripts_with_args(command: Path):
    """Test that the script is called."""
    some_files = ["file1", "file2", "file3"]
    out = run_on_files(str(command) + " -f -v -h", some_files)
    assert out
    assert out.decode().strip() == "Got -f -v -h " + " ".join(some_files)


def test_run_on_messages_passes_files_to_script(command):
    """Test that the script is called."""
    some_files = ["file1", "file2", "file3"]
    messages = [Message("some_topic", "file", data={"uri": f}) for f in some_files]
    for i, (out, _mda) in enumerate(run_on_messages(str(command), messages)):
        assert out.decode().strip() == "Got " + some_files[i]


def test_run_on_messages_passes_metadata_to_script(command: Path):
    """Test that the script is called."""
    some_files = ["file1", "file2", "file3"]
    messages = [Message("some_topic", "file", data={"uri": f,
                                                    "info": "hi",
                                                    "time": datetime(2025, 4, 9, 19, 12, 52)}) for f in some_files]
    command_to_call = os.fspath(command) + " {info} -s {time:%Y%m%dT%H%M%S}"
    for i, (out, _mda) in enumerate(run_on_messages(command_to_call, messages)):
        assert out.decode().strip() == "Got hi -s 20250409T191252 " + some_files[i]

def test_run_on_messages_passes_dataset_to_script(command):
    """Test that the script is called."""
    some_files = ["file1", "file2", "file3"]
    data = {"dataset": [{"uri": f} for f in some_files]}
    messages = [Message("some_topic", "dataset", data=data)]
    for out, _mda in run_on_messages(command, messages):
        assert out.decode().strip() == "Got " + " ".join(some_files)


def test_run_on_messages_does_not_run_on_ack(command):
    """Test that run does not consider ack messages."""
    some_files = ["file1", "file2", "file3"]
    messages = [Message("some_topic", "ack", data={"uri": f}) for f in some_files]
    for _ in run_on_messages(command, messages):
        raise AssertionError


def test_run_on_messages_does_not_pass_dataset_from_ack(command):
    """Test that run does not consider ack messages."""
    some_files = ["file1", "file2", "file3"]
    data = {"dataset": [{"uri": f} for f in some_files]}
    messages = [Message("some_topic", "ack", data=data)]
    for _ in run_on_messages(command, messages):
        raise AssertionError


def test_run_starts_and_stops_subscriber(command):
    """Test that run starts and stops a subscriber."""
    subscriber_settings = dict(nameserver=False, addresses=["ipc://bla"])
    with mock.patch("pytroll_runner.create_subscriber_from_dict_config") as subscriber_creator:
        subscriber_creator.return_value.recv.return_value = []
        for _ in run_from_new_subscriber(command, subscriber_settings):
            pass
        subscriber_creator.assert_called_once_with(subscriber_settings)
        subscriber_creator.return_value.close.assert_called_once()


def test_run_on_subscriber(command):
    """Test that we run using a subscriber."""
    some_files = ["file1", "file2", "file3"]
    messages = [Message("some_topic", "file", data={"uri": f, "sensor": "thermometer"}) for f in some_files]
    subscriber_settings = dict(nameserver=False, addresses=["ipc://bla"])
    with patched_subscriber_recv(messages):
        for i, (out, mda) in enumerate(run_from_new_subscriber(command, subscriber_settings)):
            assert out.decode().strip() == "Got " + some_files[i]
            assert mda["sensor"] == "thermometer"
    assert i == 2


def test_find_files_and_generate_message(tmp_path):
    """Test that multiple files generate a message with a dataset."""
    some_files = ["file1", "file2", "file3"]
    for filename in some_files:
        with open(tmp_path / filename, "w") as fd:
            fd.write("hi")
    pattern = os.fspath(tmp_path / "file?")
    pub_config = dict(publisher_settings=dict(nameserver=False, topic="/hi/there/"),
                      expected_files=pattern,
                      topic="/hi/there")

    message = generate_message_from_expected_files(pub_config)
    assert message.data["dataset"][0]["uid"] == some_files[0]
    assert message.data["dataset"][1]["uri"] == os.fspath(tmp_path / some_files[1])
    assert message.type == "dataset"


def test_find_one_file_and_generate_file_message(single_file_to_glob, tmp_path):
    """Test that one file generates a "file" message."""
    pattern = single_file_to_glob
    pub_config = dict(publisher_settings=dict(nameserver=False, topic="/hi/there/"),
                      expected_files=pattern,
                      topic="/hi/there")

    message = generate_message_from_expected_files(pub_config)
    assert message.data["uid"] == "file1"
    assert message.data["uri"] == os.fspath(tmp_path / "file1")
    assert message.type == "file"


def test_find_files_and_generate_message_with_static_metadata(files_to_glob):
    """Test that static metadata is passed to the new messages."""
    pattern = files_to_glob
    pub_config = dict(publisher_settings=dict(nameserver=False, topic="/hi/there/"),
                      expected_files=pattern,
                      static_metadata=dict(sensor="thermometer"),
                      topic="/hi/there")

    message = generate_message_from_expected_files(pub_config)
    assert message.data["sensor"] == "thermometer"


@pytest.fixture
def files_to_glob(tmp_path):
    """Create multiple files to glob."""
    some_files = ["file1", "file2", "file3"]
    for filename in some_files:
        with open(tmp_path / filename, "w") as fd:
            fd.write("hi")
    pattern = os.fspath(tmp_path / "file?")
    return pattern


def test_generate_message_uses_provided_topic(files_to_glob):
    """Test that the provided topic is used for new messages."""
    pattern = files_to_glob
    pub_config = dict(publisher_settings=dict(nameserver=False, topic="/hi/there/"),
                      expected_files=pattern,
                      topic="/hi/there")

    message = generate_message_from_expected_files(pub_config)
    assert message.subject == "/hi/there"


def test_find_files_and_generate_message_with_dynamic_metadata(files_to_glob):
    """Test that dynamic data is passed on."""
    pattern = files_to_glob
    pub_config = dict(publisher_settings=dict(nameserver=False, topic="/hi/there/"),
                      expected_files=pattern,
                      topic="/hi/there")

    extra_metadata = dict(sensor="thermometer")

    message = generate_message_from_expected_files(pub_config, extra_metadata)
    assert message.data["sensor"] == "thermometer"


def test_find_files_and_generate_message_with_both_static_and_dynamic_metadata(files_to_glob):
    """Test that both static and dynamic metadata is passed on."""
    pattern = files_to_glob
    pub_config = dict(publisher_settings=dict(nameserver=False, topic="/hi/there/"),
                      expected_files=pattern,
                      topic="/hi/there",
                      static_metadata=dict(sensor="accelerometer"))

    extra_metadata = dict(sensor="thermometer")

    message = generate_message_from_expected_files(pub_config, extra_metadata)
    assert message.data["sensor"] == "accelerometer"


def test_uri_uid_removed_from_input_mda(files_to_glob):
    """Test that uri and uid are not passed from the input message."""
    pattern = files_to_glob
    pub_config = dict(publisher_settings=dict(nameserver=False, topic="/hi/there/"),
                      expected_files=pattern,
                      topic="/hi/there")

    extra_metadata = dict(sensor="thermometer", uid="input_file", uri="/some/dir/to/input_file", dataset=[])

    message = generate_message_from_expected_files(pub_config, extra_metadata)
    assert "uri" not in message.data
    assert "uid" not in message.data


def test_dataset_removed_from_input_mda(single_file_to_glob):
    """Test that dataset is not passed from the input message."""
    pattern = single_file_to_glob
    pub_config = dict(publisher_settings=dict(nameservers=False),
                      expected_files=pattern,
                      topic="/hi/there")

    extra_metadata = dict(sensor="thermometer", dataset=[])

    message = generate_message_from_expected_files(pub_config, extra_metadata)
    assert "dataset" not in message.data


@pytest.fixture
def single_file_to_glob(tmp_path):
    """Create a single file to glob."""
    some_files = ["file1"]
    for filename in some_files:
        with open(tmp_path / filename, "w") as fd:
            fd.write("hi")
    pattern = os.fspath(tmp_path / "file?")
    return pattern


@pytest.fixture
def old_generated_file(tmp_path):
    """Create a single file to glob."""
    some_files = ["file0.bla"]
    for filename in some_files:
        with open(tmp_path / filename, "w") as fd:
            fd.write("hi")
    return filename


def test_run_and_publish(caplog, tmp_path, config_file_bla, files_to_glob):
    """Test run and publish."""
    some_files = ["file1", "file2", "file3"]
    data = {"dataset": [{"uri": os.fspath(tmp_path / f), "uid": f} for f in some_files]}
    messages = [Message("some_topic", "dataset", data=data)]

    with caplog.at_level(logging.DEBUG):
        with patched_subscriber_recv(messages):
            with patched_publisher() as published_messages:
                run_and_publish(config_file_bla)
                assert len(published_messages) == 1
                message = Message(rawstr=published_messages[0])
                for ds, filename in zip(message.data["dataset"], some_files):
                    assert ds["uid"] == filename + ".bla"

    assert "Subscriber config settings: " in caplog.text
    assert "addresses = ['ipc://bla']" in caplog.text
    assert "nameserver = False" in caplog.text


def test_run_and_publish_does_not_pick_old_files(tmp_path, config_file_bla, files_to_glob, old_generated_file):
    """Test run and publish."""
    some_files = ["file1"]
    data = {"dataset": [{"uri": os.fspath(tmp_path / f), "uid": f} for f in some_files]}
    first_message = Message("some_topic", "dataset", data=data)

    with patched_subscriber_recv([first_message]):
        with patched_publisher() as published_messages:
            run_and_publish(config_file_bla)
            assert len(published_messages) == 1
            assert Message(rawstr=published_messages[0]).data["uid"] == "file1.bla"

    some_files = ["file2"]
    data = {"dataset": [{"uri": os.fspath(tmp_path / f), "uid": f} for f in some_files]}
    second_message = Message("some_topic", "dataset", data=data)

    some_files = ["file3"]
    data = {"dataset": [{"uri": os.fspath(tmp_path / f), "uid": f} for f in some_files]}
    third_message = Message("some_topic", "dataset", data=data)

    with patched_subscriber_recv([second_message, third_message]):
        with patched_publisher() as published_messages:
            run_and_publish(config_file_bla)
            assert len(published_messages) == 2
            assert Message(rawstr=published_messages[0]).data["uid"] == "file2.bla"
            assert Message(rawstr=published_messages[1]).data["uid"] == "file3.bla"


def test_run_and_publish_with_files_from_log(tmp_path, config_file_aws):
    """Test run and publish."""
    some_files = ["file1"]
    data = {"dataset": [{"uri": os.fspath(tmp_path / f), "uid": f} for f in some_files]}
    first_message = Message("some_topic", "dataset", data=data)

    expected = "W_XX-OHB-Unknown,SAT,1-AWS-1B-RAD_C_OHB_20230817094846_G_D_20220621090100_20220621090618_T_B____.nc"

    with patched_subscriber_recv([first_message]):
        with patched_publisher() as published_messages:
            run_and_publish(config_file_aws)
            assert len(published_messages) == 1
            message = Message(rawstr=published_messages[0])
            assert message.data["uri"] == "/local_disk/aws_test/test/RAD_AWS_1B/" + expected


def test_run_and_no_publish_when_regex_unmatched(tmp_path, config_aws, caplog):
    """Test run and publish."""
    some_files = ["file1"]
    data = {"dataset": [{"uri": os.fspath(tmp_path / f), "uid": f} for f in some_files]}
    first_message = Message("some_topic", "dataset", data=data)

    config_aws["publisher_config"]["output_files_log_regex"] = r"regex_that_does_not_match_anything!"
    _config_file_aws = write_config_file(tmp_path, config_aws)
    caplog.set_level(logging.DEBUG)
    with patched_subscriber_recv([first_message]):
        with patched_publisher() as published_messages:
            run_and_publish(_config_file_aws)
            assert "We could find not any new files, so no message will be sent." in caplog.text
            assert published_messages == []


def test_run_and_publish_with_faulty_config(tmp_path, config_aws):
    """Test run and publish."""
    config_aws["publisher_config"].pop("output_files_log_regex")
    yaml_file = write_config_file(tmp_path, config_aws)

    some_files = ["file1"]
    data = {"dataset": [{"uri": os.fspath(tmp_path / f), "uid": f} for f in some_files]}
    first_message = Message("some_topic", "dataset", data=data)

    with patched_subscriber_recv([first_message]):
        with patched_publisher():
            with pytest.raises(KeyError, match="Missing ways to identify output files.*"):
                run_and_publish(yaml_file)


def test_config_reader(command, tmp_path):
    """Test the config reader."""
    sub_config = dict(nameserver=False, addresses=["ipc://bla"])
    pub_config = dict(publisher_settings=dict(nameserver=False, port=1979),
                      topic="/hi/there/",
                      output_files_log_regex="Written output file : (.*.nc)")
    command_path = os.fspath(command)
    test_config = dict(subscriber_config=sub_config,
                       script=command_path,
                       publisher_config=pub_config)
    yaml_file = tmp_path / "config.yaml"
    with open(yaml_file, "w") as fd:
        fd.write(yaml.dump(test_config))
    command_to_call, subscriber_config, publisher_config = read_config(yaml_file)

    assert subscriber_config == sub_config
    assert command_to_call == command_path
    assert publisher_config == pub_config


def test_main_crashes_when_config_missing():
    """Test that main crashes when config is missing."""
    with pytest.raises(SystemExit):
        main([])


def test_main_crashes_when_config_file_missing():
    """Test that main crashes when the config file is missing."""
    with pytest.raises(FileNotFoundError):
        main(["moose_config.yaml"])


def test_main_parse_log_configfile(config_file_aws, log_config_file):
    """Test that when parsing a log-config yaml file logging is being setup."""
    messages = []
    with patched_subscriber_recv(messages):
        with patched_publisher():
            main([str(config_file_aws), "-l", str(log_config_file)])

    assert isinstance(logging.getLogger("").handlers[0], logging.StreamHandler)


@pytest.fixture
def ten_files_to_glob(tmp_path):
    """Create multiple files to glob."""
    n_files = 10
    some_files = [f"file{n}" for n in range(n_files)]
    for filename in some_files:
        with open(tmp_path / filename, "w") as fd:
            fd.write("hi")
    return some_files


def test_run_and_publish_with_command_subitem_and_thread_number(tmp_path, command_bla, ten_files_to_glob):
    """Test run and publish."""
    sub_config = dict(nameserver=False, addresses=["ipc://bla"])
    pub_config = dict(publisher_settings=dict(nameservers=False, port=1979),
                      output_files_log_regex="Written output file : (.*.bla)",
                      topic="/hi/there")
    command_path = os.fspath(command_bla)
    test_config = dict(subscriber_config=sub_config,
                       script=dict(command=command_path, workers=4),
                       publisher_config=pub_config)
    yaml_file = tmp_path / "config.yaml"
    with open(yaml_file, "w") as fd:
        fd.write(yaml.dump(test_config))

    some_files = ten_files_to_glob
    datas = [{"uri": os.fspath(tmp_path / f), "uid": f} for f in some_files]
    messages = [Message("some_topic", "file", data=data) for data in datas]

    with patched_subscriber_recv(messages):
        with patched_publisher() as published_messages:
            run_and_publish(yaml_file)
            assert len(published_messages) == 10
            res_files = [Message(rawstr=msg).data["uid"] for msg in published_messages]
            assert res_files != sorted(res_files)


def test_run_and_publish_from_message_file(tmp_path, config_file_aws):
    """Test run and publish."""
    some_files = ["file1"]
    data = {"dataset": [{"uri": os.fspath(tmp_path / f), "uid": f} for f in some_files]}
    first_message = Message("some_topic", "dataset", data=data)

    message_file = tmp_path / "first.msg"
    with open(message_file, mode="w") as fd:
        fd.write(str(first_message))
    expected = "W_XX-OHB-Unknown,SAT,1-AWS-1B-RAD_C_OHB_20230817094846_G_D_20220621090100_20220621090618_T_B____.nc"

    with patched_publisher() as published_messages:
        main([str(config_file_aws), "-m", str(message_file)])
        assert len(published_messages) == 1
        message = Message(rawstr=published_messages[0])

        assert message.data["uri"] == "/local_disk/aws_test/test/RAD_AWS_1B/" + expected

"""Tests for the pytroll runner."""
import os
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

script = """#!/bin/bash
echo "Got $*"
"""

script_bla = """#!/bin/bash
for file in $*; do
    cp "$file" "$file.bla"
done
"""

@pytest.fixture()
def command(tmp_path):
    """Make a command script that just prints out the files it got."""
    command_file = tmp_path / "myscript.sh"
    with open(command_file, "w") as fobj:
        fobj.write(script)
    os.chmod(command_file, 0o700)
    return command_file


@pytest.fixture()
def command_bla(tmp_path):
    """Make a command script that adds ".bla" to the filename."""
    command_file = tmp_path / "myscript_bla.sh"
    with open(command_file, "w") as fobj:
        fobj.write(script_bla)
    os.chmod(command_file, 0o700)
    return command_file


def test_run_on_files_passes_files_to_script(command):
    """Test that the script is called."""
    some_files = ["file1", "file2", "file3"]
    out = run_on_files(command, some_files)
    assert out.decode().strip() == "Got " + " ".join(some_files)


def test_run_on_messages_passes_files_to_script(command):
    """Test that the script is called."""
    some_files = ["file1", "file2", "file3"]
    messages = [Message("some_topic", "file", data={"uri": f}) for f in some_files]
    for i, (out, _mda) in enumerate(run_on_messages(command, messages)):
        assert out.decode().strip() == "Got " + some_files[i]


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


@pytest.fixture()
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


@pytest.fixture()
def single_file_to_glob(tmp_path):
    """Create a single file to glob."""
    some_files = ["file1"]
    for filename in some_files:
        with open(tmp_path / filename, "w") as fd:
            fd.write("hi")
    pattern = os.fspath(tmp_path / "file?")
    return pattern


def test_run_and_publish(tmp_path, command_bla, files_to_glob):
    """Test run and publish."""
    sub_config = dict(nameserver=False, addresses=["ipc://bla"])
    pub_config = dict(publisher_settings=dict(nameservers=False, port=1979),
                      expected_files=os.fspath(tmp_path / "file?.bla"),
                      topic="/hi/there")
    command_path = os.fspath(command_bla)
    test_config = dict(subscriber_config=sub_config,
                       script=command_path,
                       publisher_config=pub_config)
    yaml_file = tmp_path / "config.yaml"
    with open(yaml_file, "w") as fd:
        fd.write(yaml.dump(test_config))

    some_files = ["file1", "file2", "file3"]
    data = {"dataset": [{"uri": os.fspath(tmp_path / f), "uid": f} for f in some_files]}
    messages = [Message("some_topic", "dataset", data=data)]

    with patched_subscriber_recv(messages):
        with patched_publisher() as published_messages:
            run_and_publish(yaml_file)
            assert len(published_messages) == 1
            for ds, filename in zip(published_messages[0].data["dataset"], some_files):
                assert ds["uid"] == filename + ".bla"


def test_config_reader(command, tmp_path):
    """Test the config reader."""
    sub_config = dict(nameserver=False, addresses=["ipc://bla"])
    pub_config = dict(nameserver=False, topic="/hi/there/")
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

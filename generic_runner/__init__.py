"""Generic runner.

Example config file:

publisher_config:
  expected_files: /tmp/pytest-of-a001673/pytest-169/test_fake_publisher0/file?.bla
  publisher_settings:
    nameservers: false
    port: 1979
  static_metadata:
    sensor: thermometer
  topic: /hi/there
script: /tmp/pytest-of-a001673/pytest-169/test_fake_publisher0/myscript_bla.sh
subscriber_config:
  addresses:
  - ipc://bla
  nameserver: false


"""
import os
import sys
from contextlib import closing
from glob import glob
from subprocess import PIPE, Popen

import yaml
from posttroll.message import Message
from posttroll.publisher import create_publisher_from_dict_config
from posttroll.subscriber import create_subscriber_from_dict_config


def main():
    """Main script."""
    return run_and_publish(sys.argv[1])


def run_and_publish(config_file):
    """Run the command and publish the expected files."""
    command_to_call, subscriber_config, publisher_config = read_config(config_file)
    with closing(create_publisher_from_dict_config(publisher_config["publisher_settings"])) as pub:
        for _, mda in run_from_new_subscriber(command_to_call, subscriber_config):
            message = generate_message_from_expected_files(publisher_config, mda)
            pub.send(message)


def read_config(config_file):
    """Read the configuration file."""
    with open(config_file) as fd:
        config = yaml.safe_load(fd.read())
    return config["script"], config["subscriber_config"], config["publisher_config"]


def run_from_new_subscriber(command, subscriber_settings):
    """Run the command with files gotten from a new subscriber."""
    with closing(create_subscriber_from_dict_config(subscriber_settings)) as sub:
        return run_on_messages(command, sub.recv())


def run_on_messages(command, messages):
    """Run the command on files from messages."""
    accepted_message_types = ["file", "dataset"]
    for message in messages:
        if message.type not in accepted_message_types:
            continue
        try:  # file
            files = [message.data["uri"]]
        except KeyError:  # dataset
            files = []
            files.extend(info["uri"] for info in message.data["dataset"])
        yield run_on_files(command, files), message.data


def run_on_files(command, files):
    """Run the command of files."""
    if not files:
        return
    process = Popen([os.fspath(command), *files], stdout=PIPE)
    out, _ = process.communicate()
    return out


def generate_message_from_expected_files(pub_config, extra_metadata=None):
    """Generate a message containing the expected files."""
    filepattern = pub_config["expected_files"]
    metadata = populate_metadata(extra_metadata, pub_config.get("static_metadata", {}))

    dataset = []
    files = glob(filepattern)
    for filepath in sorted(files):
        filename = os.path.basename(filepath)
        dataset.append(dict(uid=filename, uri=filepath))

    if len(files) == 1:
        metadata.update(dataset[0])
        message_type = "file"
    else:
        metadata["dataset"] = dataset
        message_type = "dataset"
    return Message(pub_config["topic"], message_type, metadata)


def populate_metadata(extra_metadata, static_metadata):
    """Populate the metadata."""
    metadata = {}
    if extra_metadata is not None:
        metadata.update(extra_metadata)
        metadata.pop("uri", None)
        metadata.pop("uid", None)
        metadata.pop("dataset", None)
    metadata.update(static_metadata)
    return metadata

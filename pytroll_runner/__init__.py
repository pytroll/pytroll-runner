"""Generic runner.

Example config file:

publisher_config:
  # at least one of the following two needs to be provided. If both are present, expected_files will take precedence
  expected_files: /tmp/pytest-of-a001673/pytest-169/test_fake_publisher0/file?.bla
  output_files_log_regex: "Written output file : (.*.nc)"
  publisher_settings:
    nameservers: false
    port: 1979
  static_metadata:
    sensor: thermometer
  topic: /hi/there
script:
  command: /tmp/pytest-of-a001673/pytest-169/test_fake_publisher0/myscript_bla.sh
  workers: 4
subscriber_config:
  addresses:
  - ipc://bla
  nameserver: false


"""
import argparse
import logging
import logging.config
import os
import re
from contextlib import closing
from functools import partial
from glob import glob
from multiprocessing.pool import ThreadPool
from pathlib import Path
from subprocess import PIPE, STDOUT, Popen

import yaml
from posttroll.message import Message
from posttroll.publisher import create_publisher_from_dict_config
from posttroll.subscriber import create_subscriber_from_dict_config

logger = logging.getLogger("pytroll-runner")

def main(args: list[str] | None = None):
    """Main script."""
    parsed_args = parse_args(args=args)
    setup_logging(parsed_args.log_config)

    logger.info("Start generic pytroll-runner")
    return run_and_publish(parsed_args.config_file, parsed_args.message_file)


def setup_logging(config_file: str | None):
    """Setup the logging from a log config yaml file."""
    if config_file is not None:
        with open(config_file) as fd:
            log_dict = yaml.safe_load(fd.read())
            logging.config.dictConfig(log_dict)
            return


def parse_args(args: list[str] | None = None):
    """Parse command line arguments."""
    parser = argparse.ArgumentParser("Pytroll Runner",
                                     description="Automate third party software in a pytroll environment")
    parser.add_argument("config_file",
                        help="The configuration file to run on.",
                        type=Path)
    parser.add_argument("-l", "--log_config",
                        help="The log configuration yaml file.",
                        default=None)
    parser.add_argument("-m", "--message-file",
                        help="A file containing messages to process.",
                        default=None)
    return parser.parse_args(args)


def run_and_publish(config_file: Path, message_file: str | None = None):
    """Run the command and publish the expected files."""
    command_to_call, subscriber_config, publisher_config = read_config(config_file)
    preexisting_files = check_existing_files(publisher_config)

    with closing(create_publisher_from_dict_config(publisher_config["publisher_settings"])) as pub:
        pub.start()
        if message_file is None:
            gen = run_from_new_subscriber(command_to_call, subscriber_config)
        else:
            gen = run_from_message_file(command_to_call, message_file)
        for log_output, mda in gen:
            try:
                messages, preexisting_files = generate_message(publisher_config, mda, log_output, preexisting_files)
                for message in messages:
                    logger.debug(f"Sending message = {message}")
                    pub.send(str(message))
            except FileNotFoundError:
                logger.debug("We could not find any new files, so no message will be sent.")


def generate_message(publisher_config, mda, log_output, preexisting_files):
    """Generate message from either the log output or existing files."""
    try:
        messages = generate_message_from_log_output(publisher_config, mda, log_output)
    except KeyError:
        message = generate_message_from_expected_files(publisher_config, mda, preexisting_files)
        preexisting_files = check_existing_files(publisher_config)
        messages = [message]

    return messages, preexisting_files


def run_from_message_file(command_to_call, message_file):
    """Run the command on message file."""
    with open(message_file) as fd:
        messages = (Message(rawstr=line) for line in fd if line)
        yield from run_on_messages(command_to_call, messages)


def check_existing_files(publisher_config):
    """Check for previously generated files."""
    filepattern = publisher_config.get("expected_files", "")
    return set(glob(filepattern))


def read_config(config_file: Path):
    """Read the configuration file."""
    with open(config_file) as fd:
        config: dict[str, object] = yaml.safe_load(fd.read())
    return curate_config(config)


def curate_config(config):
    """Validate the configuration file."""
    publisher_config = config["publisher_config"]
    if "output_files_log_regex" not in publisher_config and "expected_files" not in publisher_config:
        raise KeyError("Missing ways to identify output files. "
                       "Either provide 'expected_files' or "
                       "'output_files_log_regex' in the config file.")

    subscriber_config = config["subscriber_config"]
    logger.debug("Subscriber config settings: ")
    for item, val in subscriber_config.items():
        logger.debug(f"{item} = {str(val)}")

    return config["script"], subscriber_config, publisher_config


def run_from_new_subscriber(command, subscriber_settings):
    """Run the command with files gotten from a new subscriber."""
    logger.debug("Run from new subscriber...")
    with closing(create_subscriber_from_dict_config(subscriber_settings)) as sub:
        yield from run_on_messages(command, sub.recv())


def run_on_messages(command, messages):
    """Run the command on files from messages."""
    try:
        num_workers = command.get("workers", 1)
    except AttributeError:
        num_workers = 1
    pool = ThreadPool(num_workers)
    run_command_on_message = partial(run_on_single_message, command)

    yield from pool.imap_unordered(run_command_on_message, select_messages(messages))


def select_messages(messages):
    """Select only valid messages."""
    accepted_message_types = ["file", "dataset"]
    for message in messages:
        if message.type not in accepted_message_types:
            continue
        yield message


def run_on_single_message(command: dict[str, str | int] | Path | str,
                          message: Message) -> tuple[bytes, dict[str, object]]:
    """Run the command on files from message."""
    metadata = message.data.copy()
    try:  # file
        files = [metadata.pop("uri")]
    except KeyError:  # dataset
        files = []
        files.extend(info["uri"] for info in metadata.pop("dataset"))
    command_to_call = get_command_to_call(command, metadata)
    return run_on_files(command_to_call, files), message.data


def get_command_to_call(command: dict[str, str | int] | Path | str, metadata: dict[str, str]) -> str:
    """Get the command string to call."""
    try:
        command_to_call = command["command"]
    except TypeError:
        command_to_call = os.fspath(command)
    return command_to_call.format(**metadata)


def run_on_files(command: str, files: list[str]) -> bytes | None:
    """Run the command of files."""
    if not files:
        return
    logger.info(f"Start running command {command} on files {files}")
    process = Popen([*command.split(), *files], stdout=PIPE, stderr=STDOUT)  # noqa: S603
    out = b""
    for line in process.stdout:
        out += b"\n" + line
        logger.debug("  " + line.decode("utf-8").rstrip())
    return out


def generate_message_from_log_output(publisher_config, mda, log_output):
    """Generate message for the filenames present in the log output."""
    new_files = get_newfiles_from_regex_and_logoutput(publisher_config["output_files_log_regex"], log_output)
    split_files = publisher_config.get("split_files")
    messages = []
    if len(new_files) > 1 and split_files:
        for newfile in new_files:
            messages.append(generate_message_from_new_files(publisher_config, [newfile], mda))
    else:
        messages.append(generate_message_from_new_files(publisher_config, new_files, mda))

    return messages


def _get_newfiles_from_regex(regex, log_output):
    """Get list of new output files from log messages."""
    logger.debug(f"Matching regex-pattern: {regex} from log output")
    nfiles = re.findall(regex, str(log_output, "utf-8"))
    logger.debug(f"Output files identified from log output: {nfiles}")
    return nfiles


def get_newfiles_from_regex_and_logoutput(regex, log_output):
    """Get the filenames using a regex-pattern on the log_output."""
    if isinstance(regex, list):
        new_files = []
        for rex in regex:
            nfiles = _get_newfiles_from_regex(rex, log_output)
            new_files = new_files + nfiles
    else:
        new_files = _get_newfiles_from_regex(regex, log_output)

    return new_files


def generate_message_from_expected_files(pub_config, extra_metadata=None, preexisting_files=None):
    """Generate a message containing the expected files."""
    new_files = find_new_files(pub_config, preexisting_files or set())
    return generate_message_from_new_files(pub_config, new_files, extra_metadata)


def generate_message_from_new_files(pub_config, new_files, extra_metadata):
    """Generate a message containing the new files."""
    if not new_files:
        raise FileNotFoundError("No new files were found.")

    metadata = populate_metadata(extra_metadata, pub_config.get("static_metadata", {}))

    dataset = []
    for filepath in sorted(new_files):
        filename = os.path.basename(filepath)
        dataset.append(dict(uid=filename, uri=filepath))
    if len(new_files) == 1:
        metadata.update(dataset[0])
        message_type = "file"
    else:
        metadata["dataset"] = dataset
        message_type = "dataset"
    return Message(pub_config["topic"], message_type, metadata)


def find_new_files(pub_config, preexisting_files):
    """Find new files matching the file pattern."""
    return check_existing_files(pub_config) - preexisting_files


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

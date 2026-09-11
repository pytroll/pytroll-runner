# pytroll-runner

The pytroll runner is a generic runner that allows to automate third party software in a pytroll environment.
The runner is listening for pytroll messages and running a user provided command on the files mentioned in the received
message. When the command had run, the resulting files are published as pytroll messages.
The published messages will contain metadata from the input message (except for uris and uids).

To start the runner: `pytroll-runner config.yaml`

When the command exits with a non-zero return code, the run is logged as an error and no message is published for it,
even if it did write some output files. The runner carries on with the next message.

### The output of the command

The standard output and the standard error of the command are merged into a single stream, logged line by line as they
arrive, and, when `output_files_log_regex` is used to identify the output files, matched against that regular
expression.

The merged stream is captured exactly as it arrives, but it is worth knowing that it does not necessarily arrive in the
order the command produced it. Standard output is block buffered when it is a pipe and standard error is not, so a
command writing to both can have all of its standard error arrive before any of its standard output. A partial line
flushed to one stream can likewise be split in two by a line written to the other, which will stop
`output_files_log_regex` from matching that line at all.

In practice this means:

- A regular expression matching a single line is safe.
- A regular expression spanning several lines is only reliable if the command writes all of those lines to the same
  stream.
- A command that writes progress or status without a trailing newline, to either stream, is best wrapped in a small
  script that sends that noise to `/dev/null`, so that it cannot cut into the lines naming the output files.

## The configuration file

The configuration file is made of three sections.

### `script`

A dictionary with:
- `command` Full path script to run, with extra options. If a message is used as source, the metadata can be passed to the command using the curly-brace python format syntax, eg `/path/to/myscript.sh -s {start_time:%Y%m%dT%H%M%S}`.
- optionally `workers` The number of workers to use for parallel processing of messages. Defaults to 1.

### `subscriber_config`

The configuration of the subscriber. The contents of this section are passed directly to posstroll's
`create_subscriber_from_dict`.

### `publisher_config`

This section contains settings on how to publish messages.

#### `expected_files`

The glob pattern of files to be expected when the script is run. Beware that any files with matching filenames will
be included in the list, and that could include files from previous runs.

#### `static_metadata`

Metadata to include in the published messages.

#### `publisher_settings`

The configuration for the publisher. The contents of this sections are passed directly to posttroll's
`create_publisher_from_dict`.

### Example

```yaml
script: /tmp/pytest-of-a001673/pytest-169/test_fake_publisher0/myscript_bla.sh
publisher_config:
  expected_files: /tmp/pytest-of-a001673/pytest-169/test_fake_publisher0/file?.bla
  publisher_settings:
    nameservers: false
    port: 1979
  static_metadata:
    sensor: thermometer
  topic: /hi/there
subscriber_config:
  addresses:
  - ipc://bla
  nameserver: false
```

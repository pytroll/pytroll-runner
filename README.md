# pytroll-runner

The pytroll runner is a generic runner that allows to automate third party software in a pytroll environment.
The runner is listening for pytroll messages and running a user provided command on the files mentioned in the received
message. When the command had run, the resulting files are published as pytroll messages.
The published messages will contain metadata from the input message (except for uris and uids).

To start the runner: `pytroll-runner config.yaml`

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

This strategy finds the output files by globbing the pattern and subtracting the files that were already there on the
previous pass, which cannot tell apart the files written by commands running at the same time. It therefore requires
`workers` to be 1, and the runner refuses to start otherwise. Use `output_files_log_regex` to process messages
concurrently.

#### `output_files_log_regex`

A regular expression with one capturing group, matched against the output (stdout and stderr combined) of the script,
to pick out the files it wrote, eg `"Written output file : (.*.nc)"`. This is the only strategy that can attribute
output files to the run that produced them, so it is the one to use with several `workers`. When both this and
`expected_files` are given, `output_files_log_regex` takes precedence.

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

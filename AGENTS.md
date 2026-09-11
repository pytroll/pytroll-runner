# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
pip install -e ".[test]"          # install with test deps (hatchling + hatch-vcs build backend)
pytest pytroll_runner/tests       # run the test suite
pytest pytroll_runner/tests/test_runner.py::test_name   # run a single test
pytest --cov=pytroll_runner pytroll_runner/tests --cov-report=xml   # what CI runs
pre-commit run -a                 # ruff lint + whitespace/yaml hooks
```

Ruff config lives in `pyproject.toml`: line length 120, google-convention docstrings required (`D`),
plus `E,W,F,I,S,B,A,PT,Q,TID`. `assert` is allowed only under `pytroll_runner/tests/`.
CI tests Python 3.11–3.13. The version is derived from git tags by hatch-vcs into
`pytroll_runner/version.py` (generated, not committed).

## Architecture

The whole runner is a single module, `pytroll_runner/__init__.py`, exposed as the `pytroll-runner`
console script. It is a generator pipeline driven by posttroll messages:

1. `read_config`/`curate_config` — load the YAML config and split it into the three top-level sections:
   `script`, `subscriber_config`, `publisher_config`. `curate_config` enforces the one real invariant:
   `publisher_config` must contain either `expected_files` or `output_files_log_regex`.
2. `run_and_publish` — opens the publisher, then picks the message source: a live posttroll subscriber
   (`run_from_new_subscriber`) or a file of raw messages passed with `-m` (`run_from_message_file`).
   Both feed `run_on_messages`.
3. `run_on_messages` — filters to `file`/`dataset` message types only and maps them over a `ThreadPool`
   of `script.workers` threads (default 1) via `imap_unordered`, so results come back out of order.
4. `run_on_single_message` — pulls the URIs out of the message (`uri` for `file`, the `dataset` list for
   `dataset`), formats the command template with the remaining message metadata
   (`{start_time:%Y%m%dT%H%M%S}`-style), appends the input files as argv, and runs it with `Popen`
   (stderr folded into stdout, streamed line by line to the debug log). Yields `(log_output, metadata)`.
5. `generate_message` — the output-file discovery has two strategies and falls back between them:
   `generate_message_from_log_output` scrapes filenames out of the captured log using
   `output_files_log_regex`, and on `KeyError` (no such key configured) it falls back to
   `generate_message_from_expected_files`, which globs `expected_files` and subtracts the set of files
   seen on the previous pass. That preexisting-files set is threaded through the loop and refreshed
   after every glob-based message — this is what prevents republishing old files, and is why the glob
   strategy is stateful while the regex strategy is not.
6. `generate_message_from_new_files` — raises `FileNotFoundError` when nothing new was found (caught in
   `run_and_publish` so no message is sent), publishes a `file` message for a single output and a
   `dataset` message for several. Input metadata is carried over except `uri`/`uid`/`dataset`, then
   `static_metadata` is merged on top.

`script` may be either a bare path/string or a dict with `command` and optional `workers`;
`get_command_to_call` handles both shapes.

## Testing conventions

Tests drive the real code end to end rather than mocking it: they write throwaway bash scripts into
`tmp_path` (see the `script_*` helpers in `test_runner.py`, which take a redirection specification so
the same script can be exercised writing to stdout or stderr), and use posttroll's
`patched_publisher` / `patched_subscriber_recv` to capture published messages and inject incoming ones.
Realistic third-party log output (e.g. the AWS L1 processor) is embedded verbatim in the test module to
exercise the regex strategy.

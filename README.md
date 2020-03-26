# cs-2550-project
## Phase 1

- LSM does not work between runs. ie. if you create a table, end the program,
and try to read from the table in a new run, it will not work.

- The instruction file must not have empty lines. Commented lines are allowed,
which start with a `#`, and *must* be the first character of the line.

- The default log file destination is `log.log`. The program will print out
many things to stdout, but these can be ignored. The log that is requested
in the project description will be outputted to this file (or it can be
specified in the parameter `-f` to the program)

## Running the program

First create the storage directory: `mkdir storage`

Then

`python3 main.py`

Run just like this and it will give you a help message.

Recommended to `rm -rf storage/*` before running the main program.

### Running unit tests

`./scripts/run_tests.sh`

To add new tests, put them in `tests/` and save as `<module_name>_test.py`.
See `tests/slotted_page_test.py` for an example.

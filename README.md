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

`python3 main1.py`

Run just like this and it will give you a help message.

Recommended to `rm -rf storage/*` before running the main program.

### Running unit tests

`./scripts/run_tests.sh`

To add new tests, put them in `tests/` and save as `<module_name>_test.py`.
See `tests/slotted_page_test.py` for an example.

## Phase 2

### Dependencies

tar with gzip must exist.

networkx (python module) is a dependency for phase 2.
This can be installed via `pip3 install networkx`, or
you can use the requirements.txt, like `pip3 install -r requirements.txt`

### Running the program

`python3 main2.py -h`

Run just like this and it will give you a help message.
The script will automatically destroy and recreate the `storage` directory.
After execution the storage directory will survive, however, so its contents
can be inspected to look at the disk status after execution. Note that entries
with negative IDs are marked as deleted.

The two required arguments are the scheduling type and the first script.
For example:

`python3 main2.py rr script.txt`

To run multiple scripts, simply list all the script file names:

`python3 main2.py rr script1.txt script2.txt script3.txt`

Shell autoexpansion also works, to run all the scripts in a directory.
For example, to run all scripts under `test_scripts/phase2/bench_conc`
with round robin:

`python3 main2.py rr test_scripts/phase2/bench_conc/*`

The only two scheduling types are `rr` and `random`.

The log (in the format as specified in phase 1) will be output in `log.log`
by default. The output file for this log can be specified with the `-f` parameter.

Some other useful output is printed to standard output.

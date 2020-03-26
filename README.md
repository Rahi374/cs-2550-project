# cs-2550-project
### Phase 1

LSM does not work between runs
- ie if you create a table, end the program, and try to read from the table in
  a new run, it will not work

The instruction file must not have empty lines. Commented lines are allowed,
which start with a `#`, and *must* be the first character of the line.

### Running the program

`python3 main.py`

Run just like this and it will give you a help message.

### Running tests

`./scripts/run_tests.sh`

To add new tests, put them in `tests/` and save as `<module_name>_test.py`.
See `tests/slotted_page_test.py` for an example.

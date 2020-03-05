# (NYI) 0.18.0
* TODO: add ./examples/ dir & mv examples from readme

# 0.17.1
* fix for select(): rm --first
* fix select() only returns 1 result
* mysqlclient version requirement dropped
* fix extract's sql usage
* fix id_lookup should return only one number or name

# 0.17.0
* simplified load hooks & wrappers paradigm
* broke a lot of stuff

# 0.16.0 & 0.16.1
* improved tests

# 0.15.0
* load now returns SQL kwarg list
* fix duplicate welcome message
* fix linkify script to use load SQL returned
* linkify script now `mv`s files and `rm`s at end in case of errors

# 0.14.2
* mv config_logger into `util`s
* break out imars-etl.filepath into filepanther (#47)

# 0.14.1
* add exception so airflow works better w/ autofs-managed NFS $AIRFLOW_HOME
* fix loading of files with '.' in basename
* create new OB.DAAC load example in `./examples/load`
* multiple improved log & error messages

# 0.14.0
* implement symlinking
* change `extract --link` to `extract --method link` w/ default as `copy`

# 0.13.1
* set more reasonable defaults on select output (unix for CLI, py_obj for API)
* + FSHookWrapper test_connection_path_with_params
* fix ingest_key unexpected effect on load hook (output fmt != input fmt)

# 0.13.0
* + `--quiet` mode
* + `--format` output option to `select` CLI

# 0.12.2
* better error message when no product_formats

# 0.12.1
* update deprecated warn() usages
* improved(?) hook + wrapper error printout
* fix unexpected "misshapen results" when 0 format_strings found
* + test for get_filepath_formats()

# 0.12.0
* fix select() API broken in 0.11.1 by args.pop(func)
* + ./scripts/ to help w/ airflow backfilling
* fix AssertionError on multiple `product_format`s introduced in 0.11.1
* better error message when cannot connect to airflow db
* incl traceback in message on hook fails
* fix handle_exception wrong # of args causing "takes 3 pos args but 4 given"

# 0.11.2
* better ERR message when no hooks in handler

# 0.11.1
* allow 0 len result for `product_format`s & print warning
* fix select() CLI usage to eat extra kwargs (like `func`)

# 0.11.0
* fixes unexpected AssertionError "inputs not in dict" w/ implicit hashcheck
* load API explicitly forbids positional args instead of confusing other errs
* + test for S3 API load usage

# 0.10.1
* + tests for get_records always returns list of tuples
* resolve many bugs created by get_records() changes in v0.10.0

# 0.10.0
* BREAKING: select() now *always* returns list of tuples eg: [(0,1)]
* fix select() expecting multiple results always fails
* update sentinel path format string
* multiple bugfixes for new select()

# 0.9.3
* even more verbose printouts on "All hooks failed"
* removed unnecessary try-except nesting for hooks & wrappers

# 0.9.2
* more verbose printouts on "All hooks failed"

# 0.9.1
* fix `if x=True` bug in extract
* fix `ValueError` in `sql_str_to_dict` when using non-space whitespace

# 0.9.0
* + boilerplate for `extract --link`
* more `lru_cache` usage
* `select` allows more SQL commands
* now (theoretically) ignoring duplicate files when same multihashes (#41)

# 0.8.8
* add `lru_cache`ing to reduce loads on metadata db

# 0.8.7
* bugfix: duplicate hashes ignored (was not fixed in 0.7.2?)
* bugfix: now supports format spec options (eg {myvar:1d}) in fmt strings
* fix(?) weird logger discrepancies between CLI & API

# 0.8.6
* bugfix: CLI --verbose log config ignored

# 0.8.1 - 0.8.5
* various fixes to make 0.8.0 usable. 0.8.0-0.8.5 completely broken.

# 0.8.0
* change using metadata db for filepath info (#33)

# 0.7.2
* bugfix: duplicate hashes now properly ignored if `--duplicates_ok`
* s3 pids 48-50 added to data.py

# 0.7.1
* date_time strings with timezone shift suffix in form +00:00 now handled

# 0.7.0
* + select `--post_where` option

# < 0.6.0
version changes not logged, see commit history.

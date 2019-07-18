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

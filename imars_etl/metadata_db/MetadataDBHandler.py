"""
Connections are managed using apache-airflow hooks.
Connections can be installed using the `airflow connections` command.
Within the code connections are wrapped in a few ways to unify backend
interactions down to two interfaces: "object_storage" and "metadata_db".

The hooks provided by airflow often have differing interfaces.
Multiple HookWrappers are provided here to wrap around one or more hook classes
and provide a consistent interface to object_storage or metadata_db backends.
Two HookHandlers (ObjectStorageHandler and MetadataDBHandler) are provided
to serve as the topmost interface.
HookHandlers apply the appropriate wrappers to hooks to provide either a
metadata or object-store interface, so users don't have to worry about
HookHandlers.

```
# OSI-like model:

        |-------------------------------------+-----------------------------|
methods | .load() .extract()                  | .get_first() .get_records() |
        | .format_filepath()                  | .insert_rows()              |
        |-------------------------------------|-----------------------------|
Handles | ObjectStorageHandler                | MetadataDBHandler           |
        |---------------+---------------------|-----------------------------|
Wrappers| FSHookWrapper | DataLakeHookWrapper |                             |
        |---------------|-----+---------------|-----------------------------|
Hooks   |       FS      | S3  | AzureDataLake | DbAPIHook                   |
        |---------------|-----|---------------|------------+-------+--------|
Backends| HD| NFS| FUSE | S3  | Azure         | MySQL      | MsSQL | SQLite |
        |---------------+-----+---------------+------------+-------+--------+
```
"""
import logging
import sys

from imars_etl.get_hook import get_hook
from imars_etl.exceptions.NoMetadataMatchException \
    import NoMetadataMatchException
from imars_etl.exceptions.TooManyMetadataMatchesException \
    import TooManyMetadataMatchesException


class MetadataDBHandler(object):
    def __init__(self, **kwargs):
        self.db_hook = get_hook(kwargs['metadata_db'])

    def insert_rows(self, *args, **kwargs):
        self.db_hook.insert_rows(*args, **kwargs)

    def get_records(
        self, sql,
        first=True, check_result=True, should_commit=False,
    ):
        """
        Parameters:
        -----------
        sql : str
            sql query to execute
        check_result : bool
            Should we check the response for possible errors? Example errors:
                * more than 1 result returned
                * less than 1 result returned
        should_commit : bool
            Connection is not autocommit by default so you must commit to
            save changes to the database.
        """
        logger = logging.getLogger("{}.{}".format(
            __name__,
            sys._getframe().f_code.co_name)
        )
        logger.setLevel(logging.DEBUG)
        logger.debug("QUERY: {}".format(sql))
        object_metadata_hook = self.db_hook

        if first is True:
            result = object_metadata_hook.get_first(sql)
        else:
            result = object_metadata_hook.get_records(sql)

        try:
            result = validate_sql_result(result)
        except (NoMetadataMatchException, TooManyMetadataMatchesException):
            if check_result is True:
                raise
        logger.debug("RESULT: {}".format(result))
        return result


def validate_sql_result(result):
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )

    if (not result):
        raise NoMetadataMatchException(
            "Zero files found matching given metadata."
        )
        # exit(EXIT_STATUS.NO_MATCHING_FILES)
    elif (len(result) > 1):
        # TODO: request more info from user?
        logger.error(result)
        raise TooManyMetadataMatchesException(
            "Too many results found matching given metadata."
        )
        # exit(EXIT_STATUS.MULTIPLE_MATCH)
    else:
        return result[0]
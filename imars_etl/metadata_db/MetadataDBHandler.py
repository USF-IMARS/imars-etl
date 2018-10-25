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

from imars_etl.BaseHookHandler import BaseHookHandler
from imars_etl.exceptions.NoMetadataMatchException \
    import NoMetadataMatchException
from imars_etl.exceptions.TooManyMetadataMatchesException \
    import TooManyMetadataMatchesException

METADATA_DB_WRAPPERS = []


class MetadataDBHandler(BaseHookHandler):
    def __init__(self, **kwargs):
        super(MetadataDBHandler, self).__init__(
            hook_conn_id=kwargs['metadata_db'],
            wrapper_classes=METADATA_DB_WRAPPERS
        )

    def insert_rows(
        self,
        table='file',
        rows=[],
        target_fields=[],
        commit_every=1000,
        replace=False,
    ):
        return self.try_hooks_n_wrappers(
            method='insert_rows',
            m_args=[],
            m_kwargs=dict(
                table=table,
                rows=rows,
                target_fields=target_fields,
                commit_every=commit_every,
                replace=replace,
            )
        )

    def get_first(self, sql):
        return self.try_hooks_n_wrappers(
            method='get_first',
            m_args=[sql],
            m_kwargs={}
        )

    def _get_records(self, sql):
        return self.try_hooks_n_wrappers(
            method='get_records',
            m_args=[sql],
            m_kwargs={}
        )

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

        if first is True:
            result = self.get_first(sql)
        else:
            result = self._get_records(sql)

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

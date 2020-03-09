from sqlalchemy import create_engine

from imars_etl.SECRETS import META_DB_URI

METADATA_ENGINE = create_engine(
    META_DB_URI
)


def meta_db_select(sql):
    with METADATA_ENGINE.connect() as con:
        result = con.execute(sql)
        # TODO: check len of result?
        return result.fetchall()


def insert(sql):
    with METADATA_ENGINE.connect() as con:
        result = con.execute(sql)
        return result


def check_result(
    result,
    min_results=1,
    max_results=-1,  # -1 means no max
    expected_columns=1
):
    # check # of results
    if (
        len(result) < min_results or
        max_results > 0 and max_results < len(result)
    ):
        if max_results < 0:  # change value for printout purposes
            max_results = "inf"
        raise AssertionError(
            "expected # results {}<N<{}. Actual #: {}".format(
                min_results,
                max_results,
                len(result)
            )
        )
    # check # of columns in each result
    if len(result[0]) != expected_columns:
        raise AssertionError(
            "expected {} columns in result. Actual #: {}".format(
                expected_columns,
                len(result[0])
            )
        )

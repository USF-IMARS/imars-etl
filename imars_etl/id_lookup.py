
from imars_etl.util import get_sql_result
from imars_etl.metadata_db import DEFAULT_METADATA_CONN_ID


def id_lookup(
    value=None,
    table=None,
    first=False,
    metadata_conn_id=DEFAULT_METADATA_CONN_ID
):
    """
    translates between numeric id numbers & short names for tables like
    area, product, etc
    """
    assert value is not None
    assert table is not None
    # are we translating id#->short_name or opposite?
    try:
        value = int(value)  # ValueError if not int (aka id-like)
        column_given = 'id'
        column_to_get = 'short_name'
    except ValueError:  # value must be a `short_name`
        value = "'"+value+"'"
        column_given = 'short_name'
        column_to_get = 'id'

    translation = get_sql_result(
        "SELECT {} FROM {} WHERE {}={}".format(
            column_to_get,
            table,
            column_given,
            value
        ),
        conn_id=metadata_conn_id,
        first=first,
    )[column_to_get]

    print(translation)
    return translation

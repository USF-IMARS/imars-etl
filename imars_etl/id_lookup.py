
from imars_etl.metadata_db.MetadataDBHandler import DEFAULT_METADATA_DB_CONN_ID
from imars_etl.metadata_db.MetadataDBHandler import MetadataDBHandler

from imars_etl.filepath.formatter_hardcoded.get_product_id \
    import get_product_id
from imars_etl.filepath.formatter_hardcoded.get_product_name \
    import get_product_name


def id_lookup(
    value=None,
    table=None,
    first=False,
    testing=False, **kwargs
):
    """
    Translates between numeric id numbers & short names for tables like
    area, product, etc.

    Example Usages:
    --------------
    id_lookup(value=3, table='area')
    """
    if not testing:
        return _id_lookup(value=value, table=table, first=first, **kwargs)
    else:
        return _test_id_lookup(
            value=value, table=table, first=first, **kwargs
        )


def _id_lookup(
    value=None,
    table=None,
    first=False,
    metadata_conn_id=DEFAULT_METADATA_DB_CONN_ID,
):
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

    metadata_db = MetadataDBHandler(
        metadata_db=metadata_conn_id,
    )
    translation = metadata_db.get_records(
        "SELECT {} FROM {} WHERE {}={}".format(
            column_to_get,
            table,
            column_given,
            value
        ),
        first=first,
    )[0]
    return translation


def _test_id_lookup(value=None, table=None, first=False):
    # are we translating id#->short_name or opposite?
    try:
        value = int(value)  # ValueError if not int (aka id-like)
        return get_product_name(value)
    except ValueError:  # value must be a `short_name`
        return get_product_id(value)

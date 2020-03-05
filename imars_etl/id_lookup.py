from functools import lru_cache

from filepanther.formatter_hardcoded.get_product_id \
    import get_product_id
from filepanther.formatter_hardcoded.get_product_name \
    import get_product_name
from imars_etl.util.config_logger import config_logger
from imars_etl.metadata_db.mysql import meta_db_select


def id_lookup(
    value=None,
    table=None,
    first=False,
    testing=False, verbose=0, **kwargs
):
    """
    Translates between numeric id numbers & short names for tables like
    area, product, etc.

    Example Usages:
    --------------
    id_lookup(value=3, table='area')
    """
    config_logger(verbose)
    if not testing:
        return _id_lookup(value=value, table=table, first=first)
    else:
        return _test_id_lookup(
            value=value, table=table, first=first, **kwargs
        )


@lru_cache(maxsize=None)
def _id_lookup(
    value=None,
    table=None,
    first=False,
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

    translation = meta_db_select(
        "SELECT {} FROM {} WHERE {}={} LIMIT 1".format(
            column_to_get,
            table,
            column_given,
            value
        )
    )
    # expect only one result
    assert len(translation) == 1
    assert len(translation[0]) == 1

    return translation[0][0]


def _test_id_lookup(value=None, table=None, first=False):
    # are we translating id#->short_name or opposite?
    try:
        value = int(value)  # ValueError if not int (aka id-like)
        return get_product_name(value)
    except ValueError:  # value must be a `short_name`
        return get_product_id(value)


from imars_etl.util import dict_to_argparse_namespace
from imars_etl.util import get_sql_result


def id_lookup(args_ns):
    """
    translates between numeric id numbers & short names for tables like
    area, product, etc
    """
    if isinstance(args_ns, dict):  # args can be dict
        args_ns = dict_to_argparse_namespace(args_ns)

    # are we translating id#->short_name or opposite?
    try:
        value = int(args_ns.value)
        column_given = 'id'
        column_to_get = 'short_name'
    except ValueError:
        value = "'"+args_ns.value+"'"
        column_given = 'short_name'
        column_to_get = 'id'

    translation = get_sql_result(
        "SELECT {} FROM {} WHERE {}={}".format(
            column_to_get,
            args_ns.table,
            column_given,
            value
        ),
        first=getattr(args_ns, "first", False),
    )[column_to_get]

    print(translation)
    return translation

import logging
import sys

from imars_etl.util import dict_to_argparse_namespace, get_sql_result
from imars_etl.util.exit_status import EXIT_STATUS

def id_lookup(args):
    """
    """
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    if isinstance(args, dict):  # args can be dict
        args = dict_to_argparse_namespace(args)

    # are we translating id#->short_name or opposite?
    try:
        value = int(args.value)
        column_given = 'id'
        column_to_get= 'short_name'
    except ValueError as v_err:
        value = "'"+args.value+"'"
        column_given = 'short_name'
        column_to_get= 'id'

    translation = get_sql_result(
        args,
        "SELECT {} FROM {} WHERE {}={}".format(
            column_to_get,
            args.table,
            column_given,
            value
        )
    )[column_to_get]

    print(translation)
    return translation

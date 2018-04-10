import logging
import sys

from imars_etl.util import dict_to_argparse_namespace, print_and_return_sql

def get_metadata(args):
    """
    prints json-formatted metadata for first entry in given args.sql

    args can be dict or argparse.Namespace

    Example usage:
        ./imars-etl.py -vvv get_metadata 'area_id=1'

    returns:
    --------
    metadata : dict
        metadata from db
    """
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    if isinstance(args, dict):  # args can be dict
        args = dict_to_argparse_namespace(args)

    return print_and_return_sql(
        args,
        "SELECT * FROM file WHERE {}".format(args.sql)
    )

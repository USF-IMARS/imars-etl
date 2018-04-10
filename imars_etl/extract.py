import logging
import sys

from imars_etl.util import dict_to_argparse_namespace, get_sql_result
from imars_etl.util.exit_status import EXIT_STATUS

def extract(args):
    """
    args can be dict or argparse.Namespace

    Example usage:
        ./imars-etl.py -vvv extract 'area_id=1'
    """
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )

    if isinstance(args, dict):  # args can be dict
        args = dict_to_argparse_namespace(args)

    result = get_sql_result(
        args,
        "SELECT filepath FROM file WHERE {}".format(args.sql)
    )
    # TODO: download & then print a path to where the file can be
    # accessed on the local machine.
    # NOTE: currently this just gives the path because we assume:
    #   1. that the path is in /srv/imars-objects
    #   2. that the system is set up to access /srv/imars-objects
    print result
    return result

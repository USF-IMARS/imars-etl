import logging
import sys

from imars_etl.util import dict_to_argparse_namespace, get_sql_result
from imars_etl.drivers.imars_objects.extract_file import extract_file

STORAGE_DRIVERS = {  # map from input strings to extract fn for each backend
    'imars_objects': extract_file,
}

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
    # use driver to download & then print a path to where the file can be
    # accessed on the local machine.
    fpath = STORAGE_DRIVERS[
        getattr(args,'storage_driver','imars_objects')
    ](file, vars(**args))

    print(fpath)
    return fpath

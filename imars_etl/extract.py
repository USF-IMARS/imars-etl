import logging
import sys
import os

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
        args_dict = args
        args_ns = dict_to_argparse_namespace(argvs)
    else:  # assume we have an argparse namespace
        args_dict = vars(args)
        args_ns = args

    result = get_sql_result(
        args_ns,
        "SELECT filepath FROM file WHERE {}".format(args.sql)
    )
    src_path = result['filepath']

    if args_dict['output_path'] is None:
        args_dict['output_path'] = "./" + os.path.basename(src_path)

    # use driver to download & then print a path to where the file can be
    # accessed on the local machine.
    fpath = STORAGE_DRIVERS[
        args_dict['storage_driver']
    ](src_path, args_dict['output_path'], **vars(args))

    print(fpath)
    return fpath

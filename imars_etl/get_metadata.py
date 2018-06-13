
from imars_etl.util import dict_to_argparse_namespace
from imars_etl.util import get_sql_result


def get_metadata(args):
    """
    Prints json-formatted metadata for first entry in given args.sql

    args can be dict or argparse.Namespace

    Example usage:
        ./imars-etl.py -vvv get_metadata 'area_id=1'

    returns:
    --------
    metadata : dict
        metadata from db

    """
    if isinstance(args, dict):  # args can be dict
        args = dict_to_argparse_namespace(args)

    result = get_sql_result(
        args,
        "SELECT * FROM file WHERE {}".format(args.sql)
    )
    print(result)
    return result

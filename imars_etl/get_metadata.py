
from imars_etl.util import dict_to_argparse_namespace
from imars_etl.util import get_sql_result


def get_metadata(args_ns):
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
    if isinstance(args_ns, dict):  # args can be dict
        args_ns = dict_to_argparse_namespace(args_ns)

    result = get_sql_result(
        "SELECT * FROM file WHERE {}".format(args_ns.sql),
        first=getattr(args_ns, "first", False),
    )
    print(result)
    return result

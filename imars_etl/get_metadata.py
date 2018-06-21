
from imars_etl.util import get_sql_result


def get_metadata(sql, first=False, **kwargs):
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
    sql_query = "SELECT * FROM file WHERE {};".format(sql)
    print(sql_query)
    result = get_sql_result(
        sql_query,
        first=first,
    )
    print(result)
    return result

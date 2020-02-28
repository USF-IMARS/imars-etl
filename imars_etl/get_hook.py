import logging
from airflow import settings
from airflow.models import Connection


CONN_ID_METADATA = 'imars_product_metadata'


def get_metadata_hook():
    return get_hook(CONN_ID_METADATA)


def get_hook(conn_id):
    """
    Get hook from airflow connections to the metadata db.
    """
    logger = logging.getLogger("imars_etl.{}".format(
        __name__,
        )
    )
    # fetch encrypted connection hook from airflow:
    session = settings.Session()
    conn = (
        session.query(Connection)
        .filter(Connection.conn_id == conn_id)
        .one()
    )
    logger.trace("conn from airflow: {}".format(conn))
    hook = conn.get_hook()
    logger.trace("hook from airflow: {}".format(hook))
    return hook

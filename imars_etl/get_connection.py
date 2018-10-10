from airflow import settings
from airflow.models import Connection


def get_connection(conn_id):
    """get connection object by id string"""
    session = settings.Session()
    return (
        session.query(Connection)
        .filter(Connection.conn_id == conn_id)
        .one()
    )

from airflow import settings
from airflow.models import Connection
from sqlalchemy.orm import exc

from imars_etl.connections import connections


def setup_connections():
    """
    Sets up all default connections read in from dict above.
    """
    session = settings.Session()
    for conn_key, conn_kwargs in connections.items():
        if not has_connection(session, conn_kwargs['conn_id']):
            add_connection(session, **conn_kwargs)


def has_connection(session, conn_id):
    try:
        (
            session.query(Connection)
            .filter(Connection.conn_id == conn_id)
            .one()
        )
    except exc.NoResultFound:
        return False
    return True


def add_connection(session, **kwargs):
    """
    conn_id, conn_type, extra, host, login,
    password, port, schema, uri
    """
    session.add(Connection(**kwargs))
    session.commit()

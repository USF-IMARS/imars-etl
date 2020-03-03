from sqlalchemy import create_engine

from imars_etl.SECRETS import META_DB_URI

METADATA_ENGINE = create_engine(
    META_DB_URI
)


def meta_db_select(sql):
    with METADATA_ENGINE.connect() as con:
        result = con.execute(sql)
        # TODO: check len of result?
        return result.fetchone()


def insert(sql):
    with METADATA_ENGINE.connect() as con:
        result = con.execute(sql)
        return result

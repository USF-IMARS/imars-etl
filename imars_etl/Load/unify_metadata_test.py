# std modules:
from unittest import TestCase

from imars_etl.Load.unify_metadata import sql_str_to_dict


class Test_sql_str_to_dict(TestCase):
    def test_multi_sql_stmt_value_error(self):
        """
        test for unexpected error condition from airflow
        ingest_ftp_na.ingest_s3a_ol_1_efr
        first observed 2019-07-10

        example of error
        ----------------
        File "/opt/imars_etl/imars_etl/Load/unify_metadata.py", line 122,
        in sql_str_to_dict
            key, val = pair.split('=')
        ValueError: too many values to unpack (expected 2)
        """
        SQL = """                         status_id=3 AND
                        area_id=12 AND
                        provenance='af-ftp_v1'                     """
        self.assertEqual(
            {"status_id": 3, "area_id": 12, "provenance": "af-ftp_v1"},
            sql_str_to_dict(SQL)
        )

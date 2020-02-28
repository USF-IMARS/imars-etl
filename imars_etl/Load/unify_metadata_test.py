# std modules:
from unittest import TestCase

from imars_etl.Load.unify_metadata import sql_str_to_dict


class Test_unify_metadata(TestCase):
    def test_unify_metadata_can_read_from_filepath(self):
        """unify meta reads filepath data"""
        from imars_etl.Load.unify_metadata import unify_metadata
        result = unify_metadata(
            filepath=(
                '/srv/imars-objects/west_fl_pen/worldview/'
                'WV02_20091220161049_103001000366EE00_09DEC20161049-M1BS-'
                '502573785040_01_P001.ntf'
            ),
            load_format=(
                'WV02_%Y%m%d%H%M%S_{someNumber}_'
                '%y%b%d%H%M%S-{m_or_p}1BS-{idNumber}_P{passNumber}.ntf'
            )
        )
        expected_subset = {
            'm_or_p': 'M',
            'idNumber': '502573785040_01',
            'passNumber': '001'
        }
        print(result)
        assert expected_subset.items() <= result.items()


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

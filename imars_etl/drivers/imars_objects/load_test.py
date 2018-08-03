"""
tests of satfilename interface.
Test args should be fully filled because imars_etl.load wraps this and
takes care of any argument checking & auto-filling.
"""

# std modules:
from unittest import TestCase
from datetime import datetime

# dependencies:
from imars_etl.drivers.imars_objects.load_file import load_file


class Test_imars_obj_load(TestCase):

    # Tests:
    #########################
    # === bash CLI
    def test_load_zip_wv2_ftp_ingest(self):
        """
        Load zip_wv2_ftp_ingest
        """
        test_args = {
            "verbose": 3,
            "dry_run": True,
            "filepath": (
                "/srv/imars-objects/ftp-ingest" +
                "/wv2_2017-03-01T2233_RB2.zip"
            ),
            "product_id": 6,
            "time": "2017-03-01T22:33",
            "datetime": datetime(2017, 3, 1, 22, 33),
            "forced_basename": "wv2_2017-03-01T2233_RB2.zip",
            "json": '{"status_id":1,"area_id":1}',
            "tag": "RB2"
        }
        self.assertEqual(
            load_file(**test_args),
            '/srv/imars-objects/zip_wv2_ftp_ingest/wv2_2017-03-01T2233_RB2.zip'
        )

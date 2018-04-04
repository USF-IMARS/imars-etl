"""
tests of satfilename interface.
Test args should be fully filled because imars_etl.load wraps this and
takes care of any argument checking & auto-filling.
"""

# std modules:
from unittest import TestCase
try:
    # py2
    from mock import MagicMock
except ImportError:
    # py3
    from unittest.mock import MagicMock
from datetime import datetime

# dependencies:
from imars_etl.drivers.imars_objects.load import load_file

class Test_imars_obj_load(TestCase):

    # tests:
    #########################
    # === bash CLI
    def test_load_zip_wv2_ftp_ingest(self):
        """
        load zip_wv2_ftp_ingest
        """
        test_args = {
            "verbose":3,
            "dry_run":True,
            "filepath":"/srv/imars-objects/ftp-ingest/wv2_2017_03_RB2.zip",
            "product_type_id":6,
            "date":"2017-03-01T00:00",
            "datetime": datetime(2017,3,1),
            "forced_basename": "wv2_2017_03_RB2.zip",
            "json":'{"status":1,"area_id":1}',
            "tag": "RB2"
        }
        self.assertEqual(
            load_file(test_args),
            '/srv/imars-objects/zip_wv2_ftp_ingest/wv2_2017_03_RB2.zip'
        )

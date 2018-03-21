"""
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
from imars_etl.drivers.imars_objects.load import _load

class Test_imars_obj_load(TestCase):

    # tests:
    #########################
    # === bash CLI
    def test_load_basic(self):
        """
        """
        test_args = {
            "verbose":0,
            "dry_run":True,
            "filepath":"/srv/imars-objects/ftp-ingest/wv2_2017_03_RB2.zip",
            "product_type_id":6,
            "date":"2017-03-01T00:00",
            "datetime": datetime(2017,3,1),
            "forced_basename": "wv2_2017_03_RB2.zip",
            "json":'{"status":1,"area_id":1}'
        }
        self.assertEqual(
            _load(**test_args),
            [   'mv',
                '/srv/imars-objects/ftp-ingest/wv2_2017_03_RB2.zip',
                '/srv/imars-objects/zip_wv2_ftp_ingest/wv2_2017_03_RB2.zip'
            ]
        )
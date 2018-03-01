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
from imars_etl.filepath.parse import _parse_date

class Test_check_match(TestCase):

    # tests:
    #########################
    def test_parse_date_no_args(self):
        """ _parse_date works for pattern w/ no-named-args """
        self.assertEqual(
            _parse_date(
                "wv2_2000_01_.zip",
                "wv2_%Y_%m_.zip"
            ),
            "2000-01-01T00:00:00"
        )
    def test_parse_date_w_args(self):
        """ _parse_date works for pattern w/ 1 regex named arg """
        self.assertEqual(
            _parse_date(
                "wv2_2000_01_myTagHere.zip",
                "wv2_%Y_%m_{tag}.zip"
            ),
            "2000-01-01T00:00:00"
        )

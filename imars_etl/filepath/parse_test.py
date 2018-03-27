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
from imars_etl.filepath.parse_param import _parse_date, parse_all_from_filename
from imars_etl.filepath.data import get_product_id


class Test_check_match(TestCase):

    # tests:
    #########################
    # === parse_all_from_filename
    def test_parse_filename_browse_jpg(self):
        """parse_all_from_filename on jpg_wv2_m1bs *-BROWSE.jpg """
        test_args = MagicMock(
            verbose=3,
            dry_run=True,
            filepath="16FEB12162518-M1BS-057488585010_01_P003-BROWSE.JPG",
            product_type_id=get_product_id("jpg_wv2_m1bs"),
        )
        res_args = parse_all_from_filename(test_args)
        self.assertEqual(
            res_args.datetime,
            datetime(2016,2,12,16,25,18)
        )

    # === _parse_date
    def test_parse_date_no_args(self):
        """ _parse_date works for pattern w/ no-named-args """
        self.assertEqual(
            _parse_date(
                "wv2_2000_01_.zip",
                "wv2_%Y_%m_.zip"
            ).isoformat(),
            "2000-01-01T00:00:00"
        )
    def test_parse_date_w_args(self):
        """ _parse_date works for pattern w/ 1 regex named arg """
        self.assertEqual(
            _parse_date(
                "wv2_2000_01_myTagHere.zip",
                "wv2_%Y_%m_{tag}.zip"
            ).isoformat(),
            "2000-01-01T00:00:00"
        )

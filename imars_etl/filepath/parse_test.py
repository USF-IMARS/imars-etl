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
from imars_etl.filepath.parse_param import parse_all_from_filename
from imars_etl.filepath.data import get_product_id


class Test_parse_param(TestCase):

    # tests:
    #########################
    # === parse_all_from_filename
    def test_parse_filename_browse_jpg(self):
        """parse_all_from_filename on jpg_wv2_m1bs *-BROWSE.jpg """
        test_args = MagicMock(
            verbose=3,
            dry_run=True,
            filepath="16FEB12162518-M1BS-057488585010_01_P003-BROWSE.JPG",
            product_type_id=None,
            product_type_name=None,
            ingest_key=None,
        )
        res_args = parse_all_from_filename(test_args)
        self.assertEqual( res_args.datetime, datetime(2016,2,12,16,25,18) )
        self.assertEqual( res_args.idNumber, "057488585010_01" )
        self.assertEqual( res_args.passNumber, "003" )
        self.assertEqual( res_args.product_type_name, "jpg_wv2_m1bs" )
        self.assertEqual(
            res_args.product_type_id,
            get_product_id("jpg_wv2_m1bs")
        )

    def test_parse_filename_shx_wv2_p1bs(self):
        """parse_all_from_filename on shx_wv2_p1bs *_PIXEL_SHAPE.shx """
        test_args = MagicMock(
            verbose=3,
            dry_run=True,
            filepath="16FEB12162518-P1BS-057488585010_01_P003_PIXEL_SHAPE.shx",
            product_type_id=None,
            product_type_name=None,
            ingest_key=None
        )
        res_args = parse_all_from_filename(test_args)
        self.assertEqual( res_args.datetime, datetime(2016,2,12,16,25,18) )
        self.assertEqual( res_args.idNumber, "057488585010_01" )
        self.assertEqual( res_args.passNumber, "003" )
        self.assertEqual( res_args.product_type_name, "shx_wv2_p1bs" )
        self.assertEqual(
            res_args.product_type_id,
            get_product_id("shx_wv2_p1bs")
        )

    def test_guess_ingest_key(self):
        """parse_all_from_filename can guess ingest_key if only 1 option"""
        test_args = MagicMock(
            verbose=3,
            dry_run=True,
            filepath="file_w_date_1997.txt",
            product_type_id=None,
            product_type_name="test_test_test",
            ingest_key=None
        )
        res_args = parse_all_from_filename(test_args)
        self.assertEqual( res_args.datetime, datetime(1997,1,1))
        self.assertEqual(
            res_args.product_type_id,
            get_product_id("test_test_test")
        )
        self.assertEqual(res_args.ingest_key, "file_w_date")

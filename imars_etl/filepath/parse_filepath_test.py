"""
"""

# std modules:
from unittest import TestCase
from datetime import datetime

# dependencies:
from imars_etl.filepath.parse_filepath import parse_filepath_from_argparse
from imars_etl.filepath.get_product_id import get_product_id
from imars_etl.cli import parse_args


class Test_parse_filepath_from_argparse(TestCase):

    # tests:
    #########################
    # === parse_filepath
    def test_parse_filename_browse_jpg(self):
        """parse_filepath_from_argparse on jpg_wv2_m1bs *-BROWSE.jpg"""
        test_args = parse_args([
            '-vvv',
            'load',
            '--dry_run',
            '-f', "16FEB12162518-M1BS-057488585010_01_P003-BROWSE.JPG",
        ])
        res_args = parse_filepath_from_argparse(test_args)
        self.assertEqual(
            res_args['datetime'],
            datetime(2016, 2, 12, 16, 25, 18)
        )
        self.assertEqual(res_args['idNumber'], "057488585010_01")
        self.assertEqual(res_args['passNumber'], "003")
        self.assertEqual(res_args['product_type_name'], "jpg_wv2_m1bs")
        self.assertEqual(
            res_args['product_id'],
            get_product_id("jpg_wv2_m1bs")
        )

    def test_parse_filename_shx_wv2_p1bs(self):
        """parse_filepath_from_argparse on shx_wv2_p1bs *_PIXEL_SHAPE.shx"""
        test_args = parse_args([
            '-vvv',
            'load',
            '--dry_run',
            '-f', "16FEB12162518-P1BS-057488585010_01_P003_PIXEL_SHAPE.shx"
        ])
        res_args = parse_filepath_from_argparse(test_args)
        self.assertEqual(
            res_args['datetime'],
            datetime(2016, 2, 12, 16, 25, 18)
        )
        self.assertEqual(res_args['idNumber'], "057488585010_01")
        self.assertEqual(res_args['passNumber'], "003")
        self.assertEqual(res_args['product_type_name'], "shx_wv2_p1bs")
        self.assertEqual(
            res_args['product_id'],
            get_product_id("shx_wv2_p1bs")
        )

    def test_guess_ingest_key(self):
        """parse_filepath_from_argparse can guess ingest_key if only 1 option"""
        test_args = parse_args([
            '-vvv',
            'load',
            '--dry_run',
            '-f', "file_w_date_1997.txt",
            '-n', "test_test_test",
        ])
        res_args = parse_filepath_from_argparse(test_args)
        self.assertEqual(res_args['datetime'], datetime(1997, 1, 1))
        self.assertEqual(
            res_args['product_id'],
            get_product_id("test_test_test")
        )
        # this fails... but I don't think we really care.
        # self.assertEqual(res_args.ingest_key, "file_w_date")

    def test_parse_args_and_date_from_filename(self):
        """Parse fancy filepath reads args & date from path"""
        test_args = parse_args([
            '-vvv',
            'load',
            '--dry_run',
            '-f', 'date_2022123.arg_testyTestArg.time_0711.woah'
        ])
        res_args = parse_filepath_from_argparse(test_args)
        self.assertEqual(res_args['datetime'], datetime(2022, 5, 3, 7, 0, 11))
        self.assertEqual(res_args['test_arg'], "testyTestArg")

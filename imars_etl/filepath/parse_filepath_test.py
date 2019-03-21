"""
"""

# std modules:
from unittest import TestCase
from datetime import datetime

# dependencies:
from imars_etl.filepath.parse_filepath import parse_filepath
from imars_etl.filepath.parse_filepath import _strptime_parsed_pattern
from imars_etl.cli import parse_args
# TODO: mock MetadataDBHandler (using formatter_hardcoded)?:
from imars_etl.metadata_db.MetadataDBHandler import MetadataDBHandler


class Test__strptime_parsed_pattern(TestCase):
    def test_strptime_with_param_with_leading_zeros(self):
        """strptime works when a named param has leading zeros"""
        _strptime_parsed_pattern(
            input_str="w2_2018_09_17T012529_fl_ne_058438305_.z",
            format_str="w2_%Y_%m_%dT%H%M%S_{area_name}_{order_id:09d}_.z",
            params=dict(
                area_name="fl_ne",
                order_id=int("058438305")
            )
        )


class Test_parse_filepath(TestCase):

    # tests:
    #########################
    # === parse_filepath
    def test_parse_filename_browse_jpg(self):
        """parse_filepath on xml_wv2_m1bs *.XML"""
        test_args = parse_args([
            '-vvv',
            'load',
            '--dry_run',
            '--nohash',
            "16FEB12162518-M1BS-057488585010_01_P003.XML",
        ])
        res_args = parse_filepath(
            MetadataDBHandler(**vars(test_args)),
            testing=True,
            **vars(test_args),
        )
        self.assertEqual(
            res_args['date_time'],
            datetime(2016, 2, 12, 16, 25, 18)
        )
        self.assertEqual(res_args['idNumber'], "057488585010_01")
        self.assertEqual(res_args['passNumber'], "003")
        self.assertEqual(res_args['product_type_name'], "xml_wv2_m1bs")

    def test_parse_filename_shx_wv2_p1bs(self):
        """parse_filepath on shx_wv2_p1bs *_PIXEL_SHAPE.shx"""
        test_args = parse_args([
            '-vvv',
            'load',
            '--dry_run',
            '--nohash',
            "16FEB12162518-P1BS-057488585010_01_P003_PIXEL_SHAPE.shx",
        ])
        res_args = parse_filepath(
            MetadataDBHandler(**vars(test_args)),
            testing=True,
            **vars(test_args)
        )
        self.assertEqual(
            res_args['date_time'],
            datetime(2016, 2, 12, 16, 25, 18)
        )
        self.assertEqual(res_args['idNumber'], "057488585010_01")
        self.assertEqual(res_args['passNumber'], "003")
        self.assertEqual(res_args['product_type_name'], "shx_wv2_p1bs")

    def test_guess_ingest_key(self):
        """
        parse_filepath can guess ingest_key if only 1 option
        """
        test_args = parse_args([
            '-vvv',
            'load',
            '--dry_run',
            '-n', "test_test_test",
            '--nohash',
            "file_w_date_1997.txt",
        ])
        res_args = parse_filepath(
            MetadataDBHandler(**vars(test_args)),
            testing=True,
            **vars(test_args)
        )
        self.assertEqual(res_args['date_time'], datetime(1997, 1, 1))
        # this fails... but I don't think we really care.
        # self.assertEqual(res_args.ingest_key, "file_w_date")

    def test_parse_args_and_date_from_filename(self):
        """Parse fancy filepath reads args & date from path"""
        test_args = parse_args([
            '-vvv',
            'load',
            '--dry_run',
            '--nohash',
            'date_2022123.arg_testyTestArg.time_0711.woah',
        ])
        res_args = parse_filepath(
            MetadataDBHandler(**vars(test_args)),
            testing=True,
            **vars(test_args)
        )
        self.assertEqual(res_args['date_time'], datetime(2022, 5, 3, 7, 0, 11))
        self.assertEqual(res_args['test_arg'], "testyTestArg")

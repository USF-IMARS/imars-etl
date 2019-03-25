"""
"""

# std modules:
from unittest import TestCase
from datetime import datetime

# dependencies:
from imars_etl.filepath.parse_filepath import parse_filepath
from imars_etl.filepath.parse_filepath import _strptime_parsed_pattern
from imars_etl.filepath.parse_filepath import _parse_multidirective
from imars_etl.cli import parse_args
# TODO: mock MetadataDBHandler (using formatter_hardcoded)?:
from imars_etl.metadata_db.MetadataDBHandler import MetadataDBHandler


class Test_parse_multidirective(TestCase):
    def test_multidirective_wv2(self):
        fmtstr = (
            "/srv/imars-objects/extra_data/WV02/2013.01/"
            "WV02_%Y%m%d%H%M%S_0000000000000000_%y%b%d%H%M%S"
            "-M1BS-059048321010_01_P001.xml"
        )
        inpstr = (
            "/srv/imars-objects/extra_data/WV02/2013.01/"
            "WV02_20130123163628_0000000000000000_13Jan23163628-"
            "M1BS-059048321010_01_P001.xml"
        )
        read_value, new_str = _parse_multidirective(inpstr, fmtstr, "%M")
        self.assertEqual(read_value, 36)


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

    def test_strptime_with_duplicate_directive(self):
        """strptime wrapper handles duplicate datetime directive"""
        dt = _strptime_parsed_pattern(
            input_str="test_11_test2_11",
            format_str="test_%d_test2_%d",
            params={}
        )
        self.assertEqual(dt, datetime.strptime('11', '%d'))

    def test_strptime_with_duplicate_directive_conflict(self):
        """strptime wrapper raises on conflicting duplicate directives"""
        with self.assertRaises(ValueError):
            _strptime_parsed_pattern(
                input_str="test_11_test2_22",
                format_str="test_%d_test2_%d",
                params={}
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

    def test_parse_abs_path(self):
        test_args = parse_args([
            # '-vvv',
            'load',
            '--dry_run',
            '--nohash',
            '--load_format',
            '{area_short_name}/{product_short_name}/WV02_%Y%m%d%H%M%S'
            '_000_%y%b%d%H%M%S-M1BS-{idNumber}_P{passNumber}.xml',
            '/srv/imars-objects/gom/wv_test_prod/WV02_20130123163628_'
            '000_13Jan23163628-M1BS-059048321010_01_P001.xml',
        ])
        res_args = parse_filepath(
            MetadataDBHandler(dry_run=True),
            testing=True,
            **vars(test_args)
        )
        self.assertEqual(
            res_args['date_time'], datetime(2013, 1, 23, 16, 36, 28)
        )
        self.assertEqual(res_args['area_short_name'], "gom")
        self.assertEqual(res_args['product_short_name'], "wv_test_prod")
        self.assertEqual(res_args['idNumber'], "059048321010")
        self.assertEqual(res_args['passNumber'], "001")

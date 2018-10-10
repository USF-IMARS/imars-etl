"""
tests of satfilename interface.
Test args should be fully filled because imars_etl.load wraps this and
takes care of any argument checking & auto-filling.
"""

# std modules:
from unittest import TestCase
from datetime import datetime

# dependencies:
from imars_etl.object_storage.IMaRSObjectsObjectHook \
    import IMaRSObjectsObjectHook


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
            "date_time": datetime(2017, 3, 1, 22, 33),
            "forced_basename": "wv2_2017-03-01T2233_RB2.zip",
            "json": '{"status_id":1,"area_id":1}',
            "tag": "RB2",
            "area_short_name": "RB2"
        }
        self.assertEqual(
            IMaRSObjectsObjectHook().load(**test_args),
            '/srv/imars-objects/RB2/zip_wv2_ftp_ingest/'
            'wv2_2017-03-01T2233_RB2.zip'
        )


class Test_format_filepath_template(TestCase):
    def test_format_filename_template(self):
        """Create basic file format template with 100% knowledge"""
        result = IMaRSObjectsObjectHook._format_filepath_template(
            "test_fancy_format_test",
            -2,
            forced_basename=None
        )
        self.assertEqual(
            result,
            "/srv/imars-objects/_fancy_{test_arg}_/%Y-%j" +
            "/arg_is_{test_arg}_time_is_%H%S.fancy_file"
        )

    def test_format_filename_w_product_id(self):
        """Can create file format template with only product_id"""
        result = IMaRSObjectsObjectHook._format_filepath_template(
            None,
            -2,
            forced_basename=None
        )
        self.assertEqual(
            result,
            "/srv/imars-objects/_fancy_{test_arg}_/%Y-%j" +
            "/arg_is_{test_arg}_time_is_%H%S.fancy_file"
        )

    def test_format_filename_w_prod_name(self):
        """Can create file format template with only product_type_name"""
        result = IMaRSObjectsObjectHook._format_filepath_template(
            "test_fancy_format_test",
            None,
            forced_basename=None
        )
        self.assertEqual(
            result,
            "/srv/imars-objects/_fancy_{test_arg}_/%Y-%j" +
            "/arg_is_{test_arg}_time_is_%H%S.fancy_file"
        )

    def test_format_filename_w_nothing(self):
        """File format template with no product info raises err"""
        with self.assertRaises(ValueError):
            IMaRSObjectsObjectHook._format_filepath_template(
                None,
                None,
                forced_basename=None
            )


class Test_format_filepath(TestCase):
    # TODO: test mismatched pid & product_name throws err?
    def test_format_filepath_p_name(self):
        """Create filepath w/ minimal args (product_name)"""
        args = {
            "date_time": datetime(2015, 5, 25, 15, 55),
            "product_type_name": "test_test_test"
        }
        result = IMaRSObjectsObjectHook().format_filepath(
            **args
        )
        self.assertEqual(
            result,
            "/srv/imars-objects/test_test_test/simple_file_with_no_args.txt"
        )

    def test_format_filepath_pid(self):
        """Create filepath w/ minimal args (product_id)"""
        args = {
            "date_time": datetime(2015, 5, 25, 15, 55),
            "product_id": -1
        }
        result = IMaRSObjectsObjectHook().format_filepath(
            **args
        )
        self.assertEqual(
            result,
            "/srv/imars-objects/test_test_test/simple_file_with_no_args.txt"
        )

    def test_format_filepath_fancy_raise(self):
        """Raise on fancy filepath missing required arg in path"""
        with self.assertRaises(KeyError):
            IMaRSObjectsObjectHook().format_filepath(
                **{
                    "date_time": datetime(2015, 5, 25, 15, 55),
                    "product_id": -2
                    # test_arg is required for this path, but is not here
                }
            )

    def test_format_fancy_filepath_w_args(self):
        """Fancy filepath w/ all required args & product_name"""
        args = {
            "date_time": datetime(2015, 5, 25, 15, 55),
            "product_type_name": "test_fancy_format_test",
            "test_arg": "myTestArg"
        }
        result = IMaRSObjectsObjectHook().format_filepath(
            **args
        )
        self.assertEqual(
            result,
            "/srv/imars-objects/_fancy_myTestArg_/2015-145" +
            "/arg_is_myTestArg_time_is_1500.fancy_file"
        )

    def test_format_fancy_filepath_w_nums(self):
        """Fancy filepath w/ all required int arg & product_name"""
        args = {
            "date_time": datetime(2015, 5, 25, 15, 55),
            "product_type_name": "test_number_format_test",
            "test_num": 3
        }
        result = IMaRSObjectsObjectHook().format_filepath(
            **args
        )
        self.assertEqual(
            result,
            "/srv/imars-objects/_fancy_003_/2015/" +
            "num_is_0003_time_is_15.fancy_file"
        )

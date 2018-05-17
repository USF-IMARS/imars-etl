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
from imars_etl.filepath.format_filepath import _format_filepath_template, format_filepath

class Test_format_filepath(TestCase):
    # TODO: test mismatched pid & product_name throws err?
    def test_format_filepath_p_name(self):
        """" create filepath w/ minimal args (product_name)"""
        args = {
            "datetime": datetime(2015,5,25,15,55),
            "product_type_name": "test_test_test"
        }
        result = format_filepath(
            args
        )
        self.assertEqual(
            result,
            "/srv/imars-objects/test_test_test/simple_file_with_no_args.txt"
        )

    def test_format_filepath_pid(self):
        """" create filepath w/ minimal args (product_id)"""
        args = {
            "datetime": datetime(2015,5,25,15,55),
            "product_id": -1
        }
        result = format_filepath(
            args
        )
        self.assertEqual(
            result,
            "/srv/imars-objects/test_test_test/simple_file_with_no_args.txt"
        )

    def test_format_filepath_fancy_raise(self):
        """ raise on fancy filepath missing required arg in path """
        with self.assertRaises(KeyError):
            result = format_filepath(
                {
                    "datetime": datetime(2015,5,25,15,55),
                    "product_id": -2
                    # test_arg is required for this path, but is not here
                }
            )

    def test_format_fancy_filepath_w_args(self):
        """" fancy filepath w/ all required args & product_name"""
        args = {
            "datetime": datetime(2015,5,25,15,55),
            "product_type_name": "test_fancy_format_test",
            "test_arg": "myTestArg"
        }
        result = format_filepath(
            args
        )
        self.assertEqual(
            result,
            "/srv/imars-objects/_fancy_myTestArg_/2015-145/arg_is_myTestArg_time_is_1500.fancy_file"
        )

class Test_format_filepath_template(TestCase):
    def test_format_filename_template(self):
        """ create basic file format template with 100% knowledge"""
        result = _format_filepath_template(
            "test_fancy_format_test",
            -2,
            forced_basename=None
        )
        self.assertEqual(
            result,
            "/srv/imars-objects/_fancy_{test_arg}_/%Y-%j/arg_is_{test_arg}_time_is_%H%S.fancy_file"
        )

    def test_format_filename_w_product_id(self):
        """ can create file format template with only product_id"""
        result = _format_filepath_template(
            None,
            -2,
            forced_basename=None
        )
        self.assertEqual(
            result,
            "/srv/imars-objects/_fancy_{test_arg}_/%Y-%j/arg_is_{test_arg}_time_is_%H%S.fancy_file"
        )

    def test_format_filename_w_prod_name(self):
        """ can create file format template with only product_type_name"""
        result = _format_filepath_template(
            "test_fancy_format_test",
            None,
            forced_basename=None
        )
        self.assertEqual(
            result,
            "/srv/imars-objects/_fancy_{test_arg}_/%Y-%j/arg_is_{test_arg}_time_is_%H%S.fancy_file"
        )

    def test_format_filename_w_nothing(self):
        """ file format template with no product info raises err """
        with self.assertRaises(ValueError):
            result = _format_filepath_template(
                None,
                None,
                forced_basename=None
            )

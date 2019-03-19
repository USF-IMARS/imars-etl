"""
tests of satfilename interface.
Test args should be fully filled because imars_etl.load wraps this and
takes care of any argument checking & auto-filling.
"""

# std modules:
from unittest import TestCase
from datetime import datetime
try:
    # py2
    from mock import MagicMock
except ImportError:
    # py3
    from unittest.mock import MagicMock

# dependencies:
from imars_etl.object_storage.hook_wrappers.FSHookWrapper \
    import FSHookWrapper


class Test_format_filepath(TestCase):
    fake_fs_hook = MagicMock(
        get_path=lambda: "/fake_path/"
    )

    # TODO: test mismatched pid & product_name throws err?
    def test_format_filepath_p_name(self):
        """Create filepath w/ minimal args (product_name)"""
        args = {
            "date_time": datetime(2015, 5, 25, 15, 55),
            "product_type_name": "test_test_test"
        }
        result = FSHookWrapper(self.fake_fs_hook).format_filepath(
            **args
        )
        self.assertEqual(
            result,
            "/fake_path/test_test_test/simple_file_with_no_args.txt"
        )

    def test_format_filepath_pid(self):
        """Create filepath w/ minimal args (product_id)"""
        args = {
            "date_time": datetime(2015, 5, 25, 15, 55),
            "product_id": -1
        }
        result = FSHookWrapper(self.fake_fs_hook).format_filepath(
            **args
        )
        self.assertEqual(
            result,
            "/fake_path/test_test_test/simple_file_with_no_args.txt"
        )

    def test_format_filepath_fancy_raise(self):
        """Raise on fancy filepath missing required arg in path"""
        with self.assertRaises(KeyError):
            FSHookWrapper(self.fake_fs_hook).format_filepath(
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
        result = FSHookWrapper(self.fake_fs_hook).format_filepath(
            **args
        )
        self.assertEqual(
            result,
            "/fake_path/_fancy_myTestArg_/2015-145" +
            "/arg_is_myTestArg_time_is_1500.fancy_file"
        )

    def test_format_fancy_filepath_w_nums(self):
        """Fancy filepath w/ all required int arg & product_name"""
        args = {
            "date_time": datetime(2015, 5, 25, 15, 55),
            "product_type_name": "test_number_format_test",
            "test_num": 3,
            "test_num2": 33
        }
        result = FSHookWrapper(self.fake_fs_hook).format_filepath(
            **args
        )
        self.assertEqual(
            result,
            "/fake_path/_fancy_003_/2015/" +
            "num_is_0033_time_is_15.fancy_file"
        )

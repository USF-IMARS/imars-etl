from unittest import TestCase

# dependencies:
from imars_etl.filepath.formatter_hardcoded.get_product_filepath_template \
    import get_product_filepath_template


class Test_get_product_filepath_template(TestCase):
    def test_format_filename_template(self):
        """Create basic file format template with 100% knowledge"""
        result = get_product_filepath_template(
            "test_fancy_format_test",
            -2,
            forced_basename=None
        )
        self.assertEqual(
            result,
            "_fancy_{test_arg}_/%Y-%j" +
            "/arg_is_{test_arg}_time_is_%H%S.fancy_file"
        )

    def test_format_filename_w_product_id(self):
        """Can create file format template with only product_id"""
        result = get_product_filepath_template(
            None,
            -2,
            forced_basename=None
        )
        self.assertEqual(
            result,
            "_fancy_{test_arg}_/%Y-%j" +
            "/arg_is_{test_arg}_time_is_%H%S.fancy_file"
        )

    def test_format_filename_w_prod_name(self):
        """Can create file format template with only product_type_name"""
        result = get_product_filepath_template(
            "test_fancy_format_test",
            None,
            forced_basename=None
        )
        self.assertEqual(
            result,
            "_fancy_{test_arg}_/%Y-%j" +
            "/arg_is_{test_arg}_time_is_%H%S.fancy_file"
        )

    def test_format_filename_w_nothing(self):
        """File format template with no product info raises err"""
        with self.assertRaises(ValueError):
            get_product_filepath_template(
                None,
                None,
                forced_basename=None
            )

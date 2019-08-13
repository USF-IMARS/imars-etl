"""
example unit test for ExampleClass
list of unittest assert methods:
https://docs.python.org/3/library/unittest.html#assert-methods
"""

# std modules:
from unittest import TestCase
from unittest.mock import MagicMock

# tested module(s):
from imars_etl.filepath.get_filepath_formats import get_filepath_formats


class Test_get_filepath_formats(TestCase):
    def test_get_filepath_format_zero_result(self):
        """ returns empty dict when no results from DB """
        mock_metadb_handle = MagicMock()
        mock_metadb_handle.get_records = MagicMock(
            return_value=[()]
        )
        result = get_filepath_formats(
            mock_metadb_handle,
            short_name="s3a_ol_1_efr", product_id=36,
            include_test_formats=False
        )
        self.assertEqual(result, {})

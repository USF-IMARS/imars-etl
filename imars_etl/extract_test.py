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

from imars_etl.cli import parse_args

class Test_extract(TestCase):

    # tests:
    #########################
    # === python API
    def test_extract_API_basic_none_found(self):
        """
        basic imars_etl.extract on with fields exits as expected:
            imars_etl.extract({
                "sql": 'status=-1 AND area_id=-10 AND product_type_id=-99'
            })
        """
        from imars_etl.extract import extract
        # result = extract()

        self.assertRaises(
            SystemExit,
            extract,
            parse_args([
                'extract',
                'status=-1 AND area_id=-10 AND product_type_id=-99'
            ])
        )

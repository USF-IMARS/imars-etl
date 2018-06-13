"""
"""

# std modules:
from unittest import TestCase

from imars_etl.cli import parse_args


class Test_extract(TestCase):

    # tests:
    #########################
    # === python API
    def test_extract_API_basic_none_found(self):
        """
        Basic imars_etl.extract on impossible query sysExits:
            imars_etl.extract({
                "sql": 'status_id=-1 AND area_id=-10 AND product_id=-99'
            })
        """
        from imars_etl.extract import extract
        # result = extract()

        self.assertRaises(
            SystemExit,
            extract,
            parse_args([
                'extract',
                'status_id=-1 AND area_id=-10 AND product_id=-99'
            ])
        )

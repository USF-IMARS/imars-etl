"""
"""

# std modules:
from unittest import TestCase


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
        from imars_etl.api import extract
        # result = extract()

        self.assertRaises(
            SystemExit,
            extract,
            'status_id=-1 AND area_id=-10 AND product_id=-99'
        )

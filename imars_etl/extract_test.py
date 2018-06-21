"""
"""

# std modules:
from unittest import TestCase

from imars_etl.exceptions.NoMetadataMatchException \
    import NoMetadataMatchException


class Test_extract_api(TestCase):

    # tests:
    #########################
    # === python API
    def test_extract_API_basic_none_found(self):
        """
        imars_etl.extract on impossible query raises NoMetadataMatchException:
            imars_etl.extract({
                "sql": 'status_id=-1 AND area_id=-10 AND product_id=-99'
            })
        """
        from imars_etl.api import extract
        # result = extract()

        self.assertRaises(
            NoMetadataMatchException,
            extract,
            'status_id=-1 AND area_id=-10 AND product_id=-99'
        )

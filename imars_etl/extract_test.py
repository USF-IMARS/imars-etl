"""
"""

# std modules:
from unittest import TestCase
from unittest.mock import patch
from unittest.mock import MagicMock


# class Test_extract_api(TestCase):
    # === python API
    # @patch(
    #     'imars_etl.get_hook.get_metadata_hook',
    #     return_value=MagicMock(
    #         name='get_records',
    #         return_value=[]
    #     )
    # )
    # def test_extract_API_basic_none_found(self, mock_get_hook):
    #     """
    #     imars_etl.extract on impossible query raises NoMetadataMatchException:
    #         imars_etl.extract({
    #             "sql": 'status_id=-1 AND area_id=-10 AND product_id=-99'
    #         })
    #     """
    #     from imars_etl.exceptions.NoMetadataMatchException \
    #         import NoMetadataMatchException
    #     from imars_etl.api import extract
    #
    #     self.assertRaises(
    #         NoMetadataMatchException,
    #         extract,
    #         'status_id=-1 AND area_id=-10 AND product_id=-99'
    #     )

    # # NOTE: this test doesn't work for unexplained reasons
    # #       the TooManyMetadataMatchesException below should be
    # #       getting caught in extract(), but it isn't.
    # @patch(
    #     'imars_etl.metadata_db.MetadataDBHandler.MetadataDBHandler.get_records'
    # )
    # @patch(
    #     'imars_etl.object_storage.ObjectStorageHandler.'
    #     'ObjectStorageHandler.extract'
    # )
    # def test_extract_TooMany_w_same_multihashes(
    #     self, mock_get_records, mock_object_extract
    # ):
    #     """
    #     imars_etl.extract() expecting one result but DB returns multiple
    #     files w/ duplicat content. extract() figures it out and returns
    #     the first.
    #     """
    #     mock_object_extract.return_value = '/fake/filepath'
    #     mock_get_records.side_effect = [
    #         # first filepath get throws exception
    #         TooManyMetadataMatchesException,
    #         # multihash get returns identical multihashes
    #         [
    #             ("QmQTg51FiwmCCUmD6mwYD7KPWN3UrwZtSPx8aJWF69MzFn"),
    #             ("QmQTg51FiwmCCUmD6mwYD7KPWN3UrwZtSPx8aJWF69MzFn")
    #         ],
    #         # final filepath get returns the filepath
    #         [('/fake/filepath')]
    #     ]
    #     from imars_etl.api import extract
    #     self.assertEqual(
    #         extract("fake sql query"),
    #         '/fake/filepath'
    #     )

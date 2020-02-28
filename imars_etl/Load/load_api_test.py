"""
"""
try:  # py2
    import mock
    from mock import patch
except ImportError:  # py3
    from unittest import mock
    from unittest.mock import patch
    from unittest.mock import MagicMock

import pytest
from imars_etl.util.TestCasePlusSQL import TestCasePlusSQL


class Test_load_api(TestCasePlusSQL):
    @patch(
        "imars_etl.Load.Load._dry_run_load_object",
        return_value="/tmp/imars-etl-test-fpath"
    )
    @patch(
        "imars_etl.Load.Load._load_metadata",
        return_value=""
    )
    @patch(
        'imars_etl.Load.validate_args._get_handles',
        return_value=(
            MagicMock(),
            MagicMock(
                name='get_records',
                return_value={}
            ),
        )
    )
    def test_load_s3(self, mock_load, mock_metadat, mock_get_handles):
        """
        test API load s3 file
        """
        import imars_etl
        imars_etl.load(
            filepath=__file__,  # just use this file as test filepath
            sql=(
                "uuid='{}' AND date_time='{}' AND product_id={} AND "
                "area_id={} AND provenance='s3_test_v1'"
            ).format(
                "fake-uuid-str", '2019-02-02 10:10:10.001', 1, 1
            ),
            dry_run=True
        )
    # === python API (passes dicts)
    # @patch(
    #     "imars_etl.object_storage.ObjectStorageHandler."
    #     "ObjectStorageHandler.load",
    #     return_value="/tmp/imars-etl-test-fpath"
    # )
    # def test_load_python_basic_dict(self, mock_load):
    #     """
    #     API basic imars_etl.load:
    #         imars_etl.load({
    #             "dry_run": True,
    #             "filepath": "/fake/path/file_w_date_2018.txt",
    #             "product_id": -1,
    #             "time": "2018-02-26T13:00",
    #             "verbose": 3
    #         })
    #     """
    #     from imars_etl.api import load
    #     res = load(
    #         dry_run=True,
    #         filepath="/fake/path/file_w_date_2018.txt",
    #         product_id=-1,
    #         time="2018-02-26T13:00",
    #         verbose=3,
    #         nohash=True,
    #     )
    #     # 'INSERT INTO file'
    #     # + ' (status_id,date_time,area_id,product_id,filepath)'
    #     # + ' VALUES (1,"2018-02-26T13:00",1,-1,'+
    #     # '"/tmp/imars-etl-test-fpath")'
    #     self.assertSQLInsertKeyValuesMatch(
    #         res,
    #         ['date_time', 'product_id', 'filepath'],
    #         [
    #             '"2018-02-26 13:00:00"',
    #             '-1',
    #             '"{}"'.format(mock_load.return_value)
    #         ]
    #     )
    #
    # @patch(
    #     "imars_etl.object_storage.ObjectStorageHandler."
    #     "ObjectStorageHandler.load",
    #     return_value="/tmp/imars-etl-test-fpath"
    # )
    # def test_load_python_with_json(self, mock_load):
    #     """
    #     API imars_etl.load with --json option
    #         imars_etl.load({
    #             "dry_run": True,
    #             "filepath": "/fake/path/file_w_date_2018.txt",
    #             "product_id": -1,
    #             "time": "2018-02-26T13:00",
    #             "json": '{"status_id":1, "area_id":1}',
    #             "verbose": 3
    #         })
    #     """
    #     from imars_etl.api import load
    #     res = load(
    #         dry_run=True,
    #         filepath="/fake/path/file_w_date_2018.txt",
    #         product_id=-1,
    #         time="2018-02-26 13:00:00",
    #         json='{"status_id":1, "area_id":1}',
    #         verbose=3,
    #         nohash=True,
    #     )
    #     self.assertSQLInsertKeyValuesMatch(
    #         res,
    #         ['status_id', 'date_time', 'area_id', 'product_id', 'filepath'],
    #         [
    #             '1',
    #             '"2018-02-26 13:00:00"',
    #             '1',
    #             '-1',
    #             '"{}"'.format(mock_load.return_value)
    #         ]
    #     )

    @patch(
        "imars_etl.Load.validate_args._get_handles",
        return_value=(1, 2)
    )
    @patch(
        "imars_etl.Load.Load._load_metadata",
        return_value=""
    )
    @patch(
        "imars_etl.Load.Load._dry_run_load_object",
        return_value="/tmp/imars-etl-test-fpath"
    )
    def test_load_att_wv2_m1bs(self, mock_load, mock_meta, mock_get_handles):
        """
        API load att_wv2_m1bs with inferred date from filepath
        """
        from imars_etl.api import load
        test_args = {
            "verbose": 3,
            "dry_run": True,
            "filepath": (
                "/tmp/airflow_output_2018-03-01T20:00:00/057522945010_01_003" +
                "/057522945010_01/057522945010_01_P002_MUL" +
                "/16FEB12162518-M1BS-057522945010_P002.ATT"
            ),
            "product_id": 7,
            # "time":"2016-02-12T16:25:18",
            # "date_time": datetime(2016,2,12,16,25,18),
            "json": '{"status_id":3,"area_id":5}',
            "nohash": True,
        }
        self.assertSQLInsertKeyValuesMatch(
            load(**test_args),
            ['status_id', 'date_time', 'area_id', 'product_id', 'filepath'],
            [
                '3',
                '"2016-02-12 16:25:18"',
                '5',
                '7',
                '"{}"'.format(mock_load.return_value)
            ]
        )

    # @patch(
    #     "imars_etl.object_storage.ObjectStorageHandler."
    #     "ObjectStorageHandler.load",
    #     return_value="/tmp/imars-etl-test-fpath"
    # )
    # def test_custom_input_load_format(self, mock_load):
    #     """
    #     imars_etl.load using manually input load_format
    #         imars_etl.load({
    #             "dry_run": True,
    #             "filepath": "/fake/path/2018_blahblah_21_06.what",
    #             "product_id": -1,
    #             "time": "2018-02-26T13:00",
    #             "verbose": 3,
    #             "load_format": ""
    #         })
    #     """
    #     from imars_etl.api import load
    #     res = load(
    #         dry_run=True,
    #         filepath="/fake/path/2018_blahblah_21_06.what",
    #         product_id=-1,
    #         # "time": "2018-02-26T13:00",
    #         verbose=3,
    #         load_format="%Y_blahblah_%d_%m.what",
    #         nohash=True,
    #     )
    #     self.assertSQLInsertKeyValuesMatch(
    #         res,
    #         ['date_time', 'product_id', 'filepath'],
    #         [
    #             '"2018-06-21 00:00:00"',
    #             '-1',
    #             '"{}"'.format(mock_load.return_value)
    #         ]
    #     )
    #
    # @patch(
    #     "imars_etl.object_storage.ObjectStorageHandler."
    #     "ObjectStorageHandler.load",
    #     return_value="/tmp/imars-etl-test-fpath"
    # )
    # def test_load_file_w_metadata_file_date(
    #     self, mock_load
    # ):
    #     """
    #     API load file w/ metadata file containing date
    #     """
    #     FAKE_UUID = '68ebc577-178e-4d9c-b16a-3bf8f1394939'
    #     DATETIME = "2018-06-20T15:36:48.227873Z"
    #     mocked_open = mock.mock_open(
    #         read_data=(
    #             '[{'
    #                 '"uuid":"' + FAKE_UUID + '",'  # noqa E131
    #                 '"indexes":[{'
    #                     '"name":"product",'  # noqa E131
    #                     '"children":[{'
    #                         '"name":"Sensing start",'  # noqa E131
    #                         '"value":"' + DATETIME + '"'
    #                     '}]'
    #                 '}]'
    #             '}]'
    #         )
    #     )
    #     with patch(
    #         'imars_etl.drivers_metadata.dhus_json.open',
    #         mocked_open, create=True
    #     ):
    #
    #         import imars_etl
    #         res = imars_etl.load(
    #             verbose=3,
    #             dry_run=True,
    #             metadata_file="/fake/metadata/filepath.json",
    #             filepath="/fake/file/path/fake_filepath.bs",
    #             product_type_name="test_fancy_format_test",
    #             ingest_key="file_w_nothing",
    #             json='{"test_arg":"tssst"}',
    #             nohash=True,
    #         )
    #         self.assertSQLInsertKeyValuesMatch(
    #             res,
    #             ['date_time', 'product_id', 'uuid', 'filepath'],
    #             [
    #                 '"{}"'.format(DATETIME[:-1].replace("T", " ")),
    #                 '-2',
    #                 '"{}"'.format(FAKE_UUID),
    #                 '"{}"'.format(mock_load.return_value)
    #             ]
    #         )

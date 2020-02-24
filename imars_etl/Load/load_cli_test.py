"""
"""
try:  # py2
    import mock
    from mock import patch
except ImportError:  # py3
    from unittest import mock
    from unittest.mock import patch

import pytest
from datetime import datetime

from imars_etl.util.TestCasePlusSQL import TestCasePlusSQL


class Test_load_cli(TestCasePlusSQL):
    # tests:
    #########################
    # === bash CLI (passes ArgParse objects)
    # @patch(
    #     "imars_etl.object_storage.ObjectStorageHandler."
    #     "ObjectStorageHandler.load"
    # )
    # def test_load_basic(self, mock_load):
    #     """
    #     CLI basic load cmd:
    #         imars_etl.py load
    #             --dry_run
    #             -p -1
    #             -p '2018-02-26T13:00'
    #             -j '{"status_id":1, "area_id":1}'
    #             /fake/path/file_w_date_2018.txt
    #     """
    #     from imars_etl.cli import main
    #     mock_load.return_value = "/tmp/imars-etl-test-fpath"
    #
    #     test_args = [
    #         '-vvv',
    #         'load',
    #         '--dry_run',
    #         '-p', '-1',
    #         '-t', "2018-02-26 13:00:00",
    #         '-j', '{"status_id":1,"area_id":1}',
    #         '--nohash',
    #         "/fake/path/file_w_date_2018.txt",
    #     ]
    #     self.assertSQLInsertKeyValuesMatch(
    #         main(test_args),
    #         ['status_id', 'date_time', 'area_id', 'product_id', 'filepath'],
    #         [
    #             '1',
    #             '"2018-02-26 13:00:00"',
    #             '1',
    #             '-1',
    #             '"{}"'.format(mock_load.return_value)
    #         ]
    #     )
    #
    # def test_load_basic_custom_obj_store(self):
    #     """
    #     CLI using `no_upload` `object_store` passes path through unchanged
    #         imars_etl.py load
    #             --dry_run
    #             --object_store no_upload
    #             -p -1
    #             -p '2018-02-26T13:00'
    #             -j '{"status_id":1, "area_id":1}'
    #             /fake/path/file_w_date_2018.txt
    #     """
    #     from imars_etl.cli import main
    #
    #     FPATH = "/fake/path/file_w_date_2018.txt"
    #
    #     test_args = [
    #         '-vvv',
    #         'load',
    #         '--dry_run',
    #         '--object_store', 'no_upload',
    #         '-p', '-1',
    #         '-t', "2018-02-26T13:00",
    #         '-j', '{"status_id":1,"area_id":1}',
    #         '--nohash',
    #         FPATH,
    #     ]
    #     self.assertSQLInsertKeyValuesMatch(
    #         main(test_args),
    #         ['status_id', 'date_time', 'area_id', 'product_id', 'filepath'],
    #         [
    #             '1',
    #             '"2018-02-26 13:00:00"',
    #             '1',
    #             '-1',
    #             '"{}"'.format(FPATH)
    #         ]
    #     )

    def test_load_missing_date_unguessable(self):
        """
        CLI cmd missing date that cannot be guessed fails:
            imars_etl.py load
                --dry_run
                -p 6
                -j '{"area_id":1}'
                '/my/path/without/a/date/in.it'
        """
        from imars_etl.cli import main
        test_args = [
            '-vvv',
            'load',
            '--dry_run',
            '-j', '{"area_id":1}',
            '-p', '6',
            '--nohash',
            "/my/path/without/a/date/in.it",
        ]
        self.assertRaises(Exception, main, test_args)  # noqa H202

    @pytest.mark.metadatatest
    @patch(
        "imars_etl.object_storage.ObjectStorageHandler."
        "ObjectStorageHandler.load"
    )
    def test_load_missing_date_guessable(self, mock_load):
        """
        CLI cmd missing date that *can* be guessed:
            imars_etl.py load
                --dry_run
                -j '{"area_id":1}'
                -p 6
                '/path/w/parseable/date/\
                wv2_1989_06_07T111234_gom_123456789_10_0.zip'
        """
        from imars_etl.cli import main
        mock_load.return_value = "/tmp/imars-etl-test-fpath"
        test_args = [
            '-vvv',
            'load',
            '--dry_run',
            '-j', '{"area_id":1}',
            '-p', '6',
            '--nohash',
            "/path/w/parseable/date/" +
            "wv2_1989_06_07T111234_gom_123456789_10_0.zip",
        ]
        self.assertSQLInsertKeyValuesMatch(
            main(test_args),
            ['date_time', 'area_id', 'product_id', 'filepath'],
            [
                '"1989-06-07 11:12:34"', '1', '6',
                '"{}"'.format(mock_load.return_value)
            ]
        )

    @pytest.mark.metadatatest
    @patch(
        "imars_etl.object_storage.ObjectStorageHandler."
        "ObjectStorageHandler.load"
    )
    def test_wv2_zip_ingest_example(self, mock_load):
        """
        CLI wv2 ingest with filename parsing:
            imars_etl.py load
                --dry_run
                -p 6
                '/path/w/parseable/date/wv2_2000_06_gom_123456789_10_0.zip'
        """
        from imars_etl.cli import main
        mock_load.return_value = "/tmp/imars-etl-test-fpath"
        test_args = [
            '-vvv',
            'load',
            '--dry_run',
            '-p', '6',
            '--nohash',
            "/path/w/parseable/date/" +
            "wv2_2000_06_07T112233_gom_123456789_10_0.zip",
        ]
        res = main(test_args)
        # 'INSERT INTO file (
        # product_id,filepath,date_time) VALUES (
        # 6,".../zip_wv2_ftp_ingest/wv2_2000-06-07T1122_m...")'
        self.assertSQLInsertKeyValuesMatch(
            res,
            ['product_id', 'filepath', 'date_time', 'area_id'],
            [
                '6',
                '"{}"'.format(mock_load.return_value),
                '"2000-06-07 11:22:33"',
                '1',
            ]
        )

    # @patch(
    #     "imars_etl.object_storage.ObjectStorageHandler."
    #     "ObjectStorageHandler.load",
    #     return_value="/tmp/imars-etl-test-fpath"
    # )
    # def test_custom_input_args_in_json(self, mock_load):
    #     """
    #     imars_etl.load w/ args passed in --json
    #     """
    #     from imars_etl.cli import main
    #     test_args = [
    #         '-vvv',
    #         'load',
    #         '--dry_run',
    #         '-n', "test_fancy_format_test",
    #         '-i', "file_w_nothing",
    #         '-t', "2018-01-01T08:08",
    #         '--json', '{"test_arg":"tssst"}',
    #         '--nohash',
    #         "fake_filepath.bs",
    #     ]
    #     res = main(test_args)
    #     self.assertSQLInsertKeyValuesMatch(
    #         res,
    #         ['date_time', 'product_id', 'filepath'],
    #         [
    #             '"2018-01-01 08:08:00"',
    #             '-2',
    #             '"{}"'.format(mock_load.return_value)
    #         ]
    #     )
    #
    # @patch(
    #     "imars_etl.object_storage.ObjectStorageHandler."
    #     "ObjectStorageHandler.load",
    #     return_value="/tmp/imars-etl-test-fpath"
    # )
    # def test_load_file_and_metadata_file(
    #     self, mock_load
    # ):
    #     """
    #     CLI load file w/ metadata file & json using default (DHUS) parser
    #     """
    #     FAKE_UUID = '68ebc577-178e-4d9c-b16a-3bf8f1394939'
    #     DATETIME = "2018-01-01T08:08:00"
    #     mocked_open = mock.mock_open(
    #         read_data='[{"uuid":"' + FAKE_UUID + '"}]'
    #     )
    #     with patch(
    #         'imars_etl.drivers_metadata.dhus_json.open',
    #         mocked_open, create=True
    #     ):
    #
    #         from imars_etl.cli import main
    #
    #         test_args = [
    #             '-vvv',
    #             'load',
    #             '--dry_run',
    #             '-m', "/fake/metadata/filepath.json",
    #             '-n', "test_fancy_format_test",
    #             '-i', "file_w_nothing",
    #             '-t', DATETIME,
    #             '--json', '{"test_arg":"tssst"}',
    #             '--nohash',
    #             "/fake/file/path/fake_filepath.bs",
    #         ]
    #
    #         res = main(test_args)
    #         self.assertSQLInsertKeyValuesMatch(
    #             res,
    #             ['date_time', 'product_id', 'uuid', 'filepath'],
    #             [
    #                 '"{}"'.format(DATETIME.replace("T", " ")),
    #                 '-2',
    #                 '"{}"'.format(FAKE_UUID),
    #                 '"{}"'.format(mock_load.return_value)
    #             ]
    #         )

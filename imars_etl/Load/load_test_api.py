"""
"""
try:  # py2
    import mock
    from mock import patch
except ImportError:  # py3
    from unittest import mock
    from unittest.mock import patch

from imars_etl.util.TestCasePlusSQL import TestCasePlusSQL


class Test_load_api(TestCasePlusSQL):
    # === python API (passes dicts)
    def test_load_python_basic_dict(self):
        """
        API basic imars_etl.load:
            imars_etl.load({
                "dry_run": True,
                "filepath": "/fake/path/file_w_date_2018.txt",
                "product_id": -1,
                "time": "2018-02-26T13:00",
                "verbose": 3
            })
        """
        from imars_etl.api import load
        res = load(
            dry_run=True,
            filepath="/fake/path/file_w_date_2018.txt",
            product_id=-1,
            time="2018-02-26T13:00",
            verbose=3,
            nohash=True,
        )
        # 'INSERT INTO file'
        # + ' (status_id,date_time,area_id,product_id,filepath)'
        # + ' VALUES (1,"2018-02-26T13:00",1,-1,'+
        # '"/srv/imars-objects/test_test_test/simple_file_with_no_args.txt")'
        self.assertSQLInsertKeyValuesMatch(
            res,
            ['date_time', 'product_id', 'filepath'],
            [
                '"2018-02-26T13:00"',
                '-1',
                '"/srv/imars-objects/test_test_test' +
                '/simple_file_with_no_args.txt"'
            ]
        )

    def test_load_python_with_json(self):
        """
        API imars_etl.load with --json option
            imars_etl.load({
                "dry_run": True,
                "filepath": "/fake/path/file_w_date_2018.txt",
                "product_id": -1,
                "time": "2018-02-26T13:00",
                "json": '{"status_id":1, "area_id":1}',
                "verbose": 3
            })
        """
        from imars_etl.api import load
        res = load(
            dry_run=True,
            filepath="/fake/path/file_w_date_2018.txt",
            product_id=-1,
            time="2018-02-26T13:00",
            json='{"status_id":1, "area_id":1}',
            verbose=3,
            nohash=True,
        )
        self.assertSQLInsertKeyValuesMatch(
            res,
            ['status_id', 'date_time', 'area_id', 'product_id', 'filepath'],
            [
                '1',
                '"2018-02-26T13:00"',
                '1',
                '-1',
                '"/srv/imars-objects/test_test_test' +
                '/simple_file_with_no_args.txt"'
            ]
        )

    def test_load_att_wv2_m1bs(self):
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
            # "datetime": datetime(2016,2,12,16,25,18),
            "json": '{"status_id":3,"area_id":5}',
            "nohash": True,
        }
        self.assertSQLInsertKeyValuesMatch(
            load(**test_args),
            ['status_id', 'date_time', 'area_id', 'product_id', 'filepath'],
            [
                '3',
                '"2016-02-12T16:25:18"',
                '5',
                '7',
                '"/srv/imars-objects/extra_data/WV02/2016.02' +
                '/WV02_20160212162518_0000000000000000_16Feb12162518-M1BS' +
                '-057522945010_P002.att"'
            ]
        )

    def test_custom_input_load_format(self):
        """
        imars_etl.load using manually input load_format
            imars_etl.load({
                "dry_run": True,
                "filepath": "/fake/path/2018_blahblah_21_06.what",
                "product_id": -1,
                "time": "2018-02-26T13:00",
                "verbose": 3,
                "load_format": ""
            })
        """
        from imars_etl.api import load
        res = load(
            dry_run=True,
            filepath="/fake/path/2018_blahblah_21_06.what",
            product_id=-1,
            # "time": "2018-02-26T13:00",
            verbose=3,
            load_format="%Y_blahblah_%d_%m.what",
            nohash=True,
        )
        self.assertSQLInsertKeyValuesMatch(
            res,
            ['date_time', 'product_id', 'filepath'],
            [
                '"2018-06-21T00:00:00"',
                '-1',
                '"/srv/imars-objects/test_test_test/' +
                'simple_file_with_no_args.txt"'
            ]
        )

    def test_load_dir(self):
        """Load directory vi python API"""
        FAKE_TEST_DIR = "/fake/dir/of/files/w/parseable/dates"
        with patch('os.walk') as mockwalk:
            from imars_etl.api import load
            mockwalk.return_value = [(
                FAKE_TEST_DIR,  # root
                (  # dirs
                ),
                (  # files
                    "file_w_date_1999.txt",
                    "date_2018333.arg_test_arg-here.time_1311.woah",
                ),
            )]
            res = load(
                product_type_name='test_fancy_format_test',
                verbose=3,
                ingest_key='file_w_date',
                directory=FAKE_TEST_DIR,
                dry_run=True,
                nohash=True,
            )

            self.assertSQLsEquals(
                res,
                [
                    ['date_time', 'product_id', 'filepath'],
                ],
                [
                    [
                        '"2018-11-29T13:00:11"',
                        '-2',
                        '"/srv/imars-objects/_fancy_test_arg-here_/2018-333' +
                        '/arg_is_test_arg-here_time_is_1311.fancy_file"'
                    ]
                ]
            )

    def test_load_file_w_metadata_file_date(
        self
    ):
        """
        API load file w/ metadata file containing date
        """
        FAKE_UUID = '68ebc577-178e-4d9c-b16a-3bf8f1394939'
        DATETIME = "2018-06-20T15:36:48.227873Z"
        mocked_open = mock.mock_open(
            read_data=(
                '[{'
                    '"uuid":"' + FAKE_UUID + '",'  # noqa E131
                    '"indexes":[{'
                        '"name":"product",'  # noqa E131
                        '"children":[{'
                            '"name":"Sensing start",'  # noqa E131
                            '"value":"' + DATETIME + '"'
                        '}]'
                    '}]'
                '}]'
            )
        )
        with patch(
            'imars_etl.drivers_metadata.dhus_json.open',
            mocked_open, create=True
        ):

            import imars_etl
            res = imars_etl.load(
                verbose=3,
                dry_run=True,
                metadata_file="/fake/metadata/filepath.json",
                filepath="/fake/file/path/fake_filepath.bs",
                product_type_name="test_fancy_format_test",
                ingest_key="file_w_nothing",
                json='{"test_arg":"tssst"}',
                nohash=True,
            )
            self.assertSQLInsertKeyValuesMatch(
                res,
                ['date_time', 'product_id', 'uuid', 'filepath'],
                [
                    '"{}"'.format(DATETIME[:19]),
                    '-2',
                    '"{}"'.format(FAKE_UUID),
                    '"/srv/imars-objects/_fancy_tssst_/2018-171' +
                    '/arg_is_tssst_time_is_1548.fancy_file"'
                ]
            )

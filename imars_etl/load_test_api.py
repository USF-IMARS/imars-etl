"""
"""

# std modules:
from unittest import TestCase
try:
    # py2
    from mock import MagicMock, patch
except ImportError:
    # py3
    from unittest.mock import MagicMock, patch
from datetime import datetime

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
        from imars_etl.load import load
        res = load({
            "dry_run": True,
            "filepath": "/fake/path/file_w_date_2018.txt",
            "product_id": -1,
            "time": "2018-02-26T13:00",
            "verbose": 3
        })
        #'INSERT INTO file'
        # + ' (status_id,date_time,area_id,product_id,filepath)'
        # + ' VALUES (1,"2018-02-26T13:00",1,-1,"/srv/imars-objects/test_test_test/simple_file_with_no_args.txt")'
        self.assertSQLInsertKeyValuesMatch(
            res,
            ['date_time','product_id','filepath'],
            [
                '"2018-02-26T13:00"',
                '-1',
                '"/srv/imars-objects/test_test_test/simple_file_with_no_args.txt"'
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
        from imars_etl.load import load
        res = load({
            "dry_run": True,
            "filepath": "/fake/path/file_w_date_2018.txt",
            "product_id": -1,
            "time": "2018-02-26T13:00",
            "json": '{"status_id":1, "area_id":1}',
            "verbose": 3
        })
        #'INSERT INTO file'
        # + ' (status_id,date_time,area_id,product_id,filepath)'
        # + ' VALUES (1,"2018-02-26T13:00",1,-1,"/srv/imars-objects/test_test_test/simple_file_with_no_args.txt")'
        self.assertSQLInsertKeyValuesMatch(
            res,
            ['status_id','date_time','area_id','product_id','filepath'],
            [
                '1',
                '"2018-02-26T13:00"',
                '1',
                '-1',
                '"/srv/imars-objects/test_test_test/simple_file_with_no_args.txt"'
            ]
        )

    def test_load_att_wv2_m1bs(self):
        """
        API load att_wv2_m1bs with inferred date from filepath
        """
        from imars_etl.load import load
        test_args = {
            "verbose":3,
            "dry_run":True,
            "filepath":"/tmp/airflow_output_2018-03-01T20:00:00/057522945010_01_003/057522945010_01/057522945010_01_P002_MUL/16FEB12162518-M1BS-057522945010_P002.ATT",
            "product_id":7,
            # "time":"2016-02-12T16:25:18",
            # "datetime": datetime(2016,2,12,16,25,18),
            "json":'{"status_id":3,"area_id":5}'
        }
        self.assertSQLInsertKeyValuesMatch(
            load(test_args),
            ['status_id','date_time','area_id','product_id','filepath'],
            [
                '3',
                '"2016-02-12T16:25:18"',
                '5',
                '7',
                '"/srv/imars-objects/extra_data/WV02/2016.02/WV02_20160212162518_0000000000000000_16Feb12162518-M1BS-057522945010_P002.att"'
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
        from imars_etl.load import load
        res = load({
            "dry_run": True,
            "filepath": "/fake/path/2018_blahblah_21_06.what",
            "product_id": -1,
            # "time": "2018-02-26T13:00",
            "verbose": 3,
            "load_format": "%Y_blahblah_%d_%m.what"
        })
        #'INSERT INTO file'
        # + ' (status_id,date_time,area_id,product_id,filepath)'
        # + ' VALUES (1,"2018-02-26T13:00",1,-1,"/srv/imars-objects/test_test_test/simple_file_with_no_args.txt")'
        self.assertSQLInsertKeyValuesMatch(
            res,
            ['date_time','product_id','filepath'],
            [
                '"2018-06-21T00:00:00"',
                '-1',
                '"/srv/imars-objects/test_test_test/simple_file_with_no_args.txt"'
            ]
        )

    def test_load_dir(self):
        """ load directory vi python API """
        FAKE_TEST_DIR="/fake/dir/of/files/w/parseable/dates"
        with patch('os.walk') as mockwalk:
            from imars_etl.load import load
            from imars_etl.cli import parse_args
            mockwalk.return_value = [(
                FAKE_TEST_DIR,  # root
                (  # dirs
                ),
                (  # files
                    "file_w_date_1999.txt",
                    "date_2018333.arg_test_arg-here.time_1311.woah",
                ),
            )]
            res = load({
                'product_type_name': 'test_fancy_format_test',
                'verbose': 3,
                'directory': FAKE_TEST_DIR,
            })

            #'INSERT INTO file'
            # + ' (status_id,date_time,area_id,product_id,filepath)'
            # + ' VALUES (1,"2018-02-26T13:00",1,-1,"/srv/imars-objects/test_test_test/simple_file_with_no_args.txt")'
            self.assertSQLInsertKeyValuesMatch(
                res,
                ['date_time','product_id','filepath'],
                [
                    '"2018-10-26T13:00:11"',
                    '-2',
                    '"/srv/imars-objects/_fancy_test_arg-here_/2018-333/arg_is_test_arg-here_time_is_1311.fancy_file'
                ]
            )

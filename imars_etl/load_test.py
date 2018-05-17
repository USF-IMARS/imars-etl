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

def parse_keys_vals_from_sql_insert(sql_str):
    # 'INSERT INTO file (
    # product_id,filepath,date_time) VALUES (
    # 6,"/srv/imars-objects/zip_wv2_ftp_ingest/wv2_2000-06-07T1122_m...")'
    pre,keys,vals = sql_str.split('(')
    keys = keys.split(')')[0].split(',')
    vals = vals.split(')')[0].split(',')
    return keys, vals

class Test_load(TestCase):
    def assertSQLInsertKeyValuesMatch(self, sql_str, keys, vals):
        """
        asserts that keys and values in SQL INSERT statement match the given.
        expected form of the sql_str is:

        'INSERT INTO file (status_id,date_time) VALUES (1,"2018-02-26T13:00")'
        """
        exp_keys,exp_vals = parse_keys_vals_from_sql_insert(sql_str)
        try:  # py3
            self.assertCountEqual(keys,exp_keys)
            self.assertCountEqual(vals,exp_vals)
        except AttributeError as a_err:
            # py2
            self.assertItemsEqual(keys, exp_keys)
            self.assertItemsEqual(vals, exp_vals)

    def assertSQLsEquals(self, sql_str_arry, keys_arry, vals_arry):
        """
        asserts that keys and values in array of SQL INSERT statements match
        the given arrays of keys & vals.
        """
        assert(len(sql_str_arry) == len(keys_arry) == len(vals_arry))
        for i,sql in enumerate(sql_str_arry):
            self.assertSQLInsertKeyValuesMatch(
                sql,
                keys_arry[i],
                vals_arry[i]
            )


    # tests:
    #########################
    # === bash CLI (passes ArgParse objects)
    def test_load_basic(self):
        """
        CLI basic load cmd:
            imars_etl.py load
                --dry_run
                -f /fake/path/file_w_date_2018.txt
                -p -1
                -p '2018-02-26T13:00'
                -j '{"status_id":1, "area_id":1}'
        """
        from imars_etl.load import load
        from imars_etl.cli import parse_args

        test_args = parse_args([
            '-vvv',
            'load',
            '--dry_run',
            '-f', "/fake/path/file_w_date_2018.txt",
            '-p', '-1',
            '-t', "2018-02-26T13:00",
            '-j', '{"status_id":1,"area_id":1}',
        ])
        self.assertSQLInsertKeyValuesMatch(
            load(test_args),
            ['status_id','date_time','area_id','product_id','filepath'],
            [
                '1',
                '"2018-02-26T13:00"',
                '1',
                '-1',
                '"/srv/imars-objects/test_test_test/simple_file_with_no_args.txt"'
            ]
        )

    def test_load_basic(self):
        """
        CLI using `no_upload` `storage_driver` passes path through unchanged
            imars_etl.py load
                --dry_run
                --storage_driver no_upload
                -f /fake/path/file_w_date_2018.txt
                -p -1
                -p '2018-02-26T13:00'
                -j '{"status_id":1, "area_id":1}'
        """
        from imars_etl.load import load
        from imars_etl.cli import parse_args

        FPATH="/fake/path/file_w_date_2018.txt"

        test_args = parse_args([
            '-vvv',
            'load',
            '--dry_run',
            '--storage_driver', 'no_upload',
            '-f', FPATH,
            '-p', '-1',
            '-t', "2018-02-26T13:00",
            '-j', '{"status_id":1,"area_id":1}',
        ])
        self.assertSQLInsertKeyValuesMatch(
            load(test_args),
            ['status_id','date_time','area_id','product_id','filepath'],
            [
                '1',
                '"2018-02-26T13:00"',
                '1',
                '-1',
                '"{}"'.format(FPATH)
            ]
        )

    def test_load_missing_date_unguessable(self):
        """
        CLI cmd missing date that cannot be guessed fails:
            imars_etl.py load
                --dry_run
                -p 6
                -j '{"area_id":1}'
                -f '/my/path/without/a/date/in.it'
        """
        from imars_etl import load
        from imars_etl.cli import parse_args
        test_args = parse_args([
            '-vvv',
            'load',
            '--dry_run',
            '-f', "/my/path/without/a/date/in.it",
            '-j', '{"area_id":1}',
            '-p', '6'
        ])
        self.assertRaises(Exception, load, test_args)

    def test_load_missing_date_guessable(self):
        """
        CLI cmd missing date that *can* be guessed:
            imars_etl.py load
                --dry_run
                -j '{"area_id":1}'
                -p 6
                -f '/path/w/parseable/date/wv2_1989-06-07T1112_myTag.zip'
        """
        from imars_etl.load import load
        from imars_etl.cli import parse_args
        test_args = parse_args([
            '-vvv',
            'load',
            '--dry_run',
            '-f', "/path/w/parseable/date/wv2_1989-06-07T1112_myTag.zip",
            '-j', '{"area_id":1}',
            '-p', '6',
        ])
        self.assertSQLInsertKeyValuesMatch(
            load(test_args),
            ['date_time','area_id','product_id','filepath'],
            [
            '"1989-06-07T11:12:00"','1','6',
            '"/srv/imars-objects/zip_wv2_ftp_ingest/wv2_1989-06-07T1112_myTag.zip"'
            ]
        )

    def test_wv2_zip_ingest_example(self):
        """
        CLI wv2 ingest with filename parsing:
            imars_etl.py load
                --dry_run
                -p 6
                -f '/path/w/parseable/date/wv2_2000_06_myTag.zip'
        """
        from imars_etl.load import load
        from imars_etl.cli import parse_args
        test_args = parse_args([
            '-vvv',
            'load',
            '--dry_run',
            '-f', "/path/w/parseable/date/wv2_2000-06-07T1122_myTag.zip",
            '-p', '6',
        ])
        res = load(test_args)
        # 'INSERT INTO file (
        # product_id,filepath,date_time) VALUES (
        # 6,"/srv/imars-objects/zip_wv2_ftp_ingest/wv2_2000-06-07T1122_m...")'
        self.assertSQLInsertKeyValuesMatch(
            res,
            ['product_id','filepath','date_time'],
            ['"/srv/imars-objects/zip_wv2_ftp_ingest/wv2_2000-06-07T1122_myTag.zip"',
                '6',
                '"2000-06-07T11:22:00"'
            ]
        )

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

    def test_load_directory_by_product_id_number(self):
        """
            CLI load directory using product_id
        """
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
                    "file_w_date_2018.txt",
                ),
            )]
            test_args = parse_args([
                '-vvv',
                'load',
                '--dry_run',
                '-d', FAKE_TEST_DIR,
                '-p', '-1',
                '-i', "file_w_date"
            ])
            self.assertSQLsEquals(
                load(test_args),
                [
                    ['date_time','product_id','filepath'],
                    ['date_time','product_id','filepath']
                ],
                [
                    [
                    '"1999-01-01T00:00:00"',
                    '-1',
                    '"/srv/imars-objects/test_test_test/simple_file_with_no_args.txt"'
                    ],
                    [
                        '"2018-01-01T00:00:00"',
                        '-1',
                        '"/srv/imars-objects/test_test_test/simple_file_with_no_args.txt"'
                    ]
                ]
            )

    def test_load_directory_short_name(self):
        """
            CLI load dir using product_type_name (short_name)
        """
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
                    "file_w_date_2018.txt",
                ),
            )]
            test_args = parse_args([
                '-vvv',
                'load',
                '--dry_run',
                '-d', FAKE_TEST_DIR,
                '-n', "test_test_test",
                '-i', "file_w_date"
            ])
            sql_strs = load(test_args)
            self.assertEqual(len(sql_strs), 2)
            self.assertSQLsEquals(
                sql_strs,
                [
                    ['date_time','product_id','filepath'],
                    ['date_time','product_id','filepath']
                ],
                [
                    ['"1999-01-01T00:00:00"','-1',
                    '"/srv/imars-objects/test_test_test/simple_file_with_no_args.txt"'
                    ],
                    ['"2018-01-01T00:00:00"','-1',
                    '"/srv/imars-objects/test_test_test/simple_file_with_no_args.txt"'
                    ]
                ]
            )

    def test_custom_input_args_in_json(self):
        """
        imars_etl.load w/ args passed in --json
        """
        from imars_etl.load import load
        from imars_etl.cli import parse_args
        test_args = parse_args([
            '-vvv',
            'load',
            '--dry_run',
            '-f', "fake_filepath.bs",
            '-n', "test_fancy_format_test",
            '-i', "file_w_nothing",
            '-t', "2018-01-01T08:08",
            '--json', '{"test_arg":"tssst"}'
        ])
        res = load(test_args)
        self.assertSQLInsertKeyValuesMatch(
            res,
            ['date_time','product_id','filepath'],
            [
                '"2018-01-01T08:08"',
                '-2',
                '"/srv/imars-objects/_fancy_tssst_/2018-001/arg_is_tssst_time_is_0800.fancy_file"'
            ]
        )


    def test_load_directory_only_loads_files_of_given_type(self):
        """
            CLI load dir loads only files that match the given type
        """
        FAKE_TEST_DIR="/fake/dir/of/files/w/parseable/dates"
        with patch('os.walk') as mockwalk:
            from imars_etl.load import load
            from imars_etl.cli import parse_args
            mockwalk.return_value = [(
                FAKE_TEST_DIR,  # root
                (  # dirs
                ),
                (  # valid files of requested type
                    "file_w_date_1999.txt",
                    "file_w_date_2018.txt",
                    # known file type that should get skipped over
                    "16FEB12162518-M1BS-057522945010_P002.ATT",
                    # invalide file name that should get skipped over
                    "my-fake_file.name.THAT_does_not-match_anyKnownFormat.EXTEN"
                ),
            )]
            test_args = parse_args([
                '-vvv',
                'load',
                '--dry_run',
                '-d', FAKE_TEST_DIR,
                '-n', "test_test_test",
                '-i', "file_w_date"
            ])
            self.assertSQLsEquals(
                load(test_args),
                [
                    ['date_time','product_id','filepath'],
                    ['date_time','product_id','filepath']
                ],
                [
                    [
                        '"1999-01-01T00:00:00"',
                        '-1',
                        '"/srv/imars-objects/test_test_test/simple_file_with_no_args.txt"'
                    ],
                    [
                        '"2018-01-01T00:00:00"',
                        '-1',
                        '"/srv/imars-objects/test_test_test/simple_file_with_no_args.txt"'
                    ]
                ]
            )

    @patch('os.walk')
    @patch(
        'imars_etl.drivers.imars_objects.load_file.load_file',
        return_value = "/fake/imars-obj/path"
    )
    def test_load_directory_leaves_unmatched_files_alone(self, mock_driver_load, mockwalk):
        """
            CLI load dir does not mess with files it cannot identify
        """
        FAKE_TEST_DIR="/fake/dir/of/files/w/parseable/dates"
        from imars_etl.load import load
        from imars_etl.cli import parse_args

        mockwalk.return_value = [(
            FAKE_TEST_DIR,  # root
            (  # dirs
            ),
            (  # valid files of requested type
                "file_w_date_1999.txt",
                "file_w_date_2018.txt",
                # known file type that should get skipped over
                "16FEB12162518-M1BS-057522945010_P002.ATT",
                # invalide file name that should get skipped over
                "my-fake_file.name.THAT_does_not-match_anyKnownFormat.EXTEN"
            ),
        )]
        test_args = parse_args([
            '-vvv',
            'load',
            '--dry_run',
            '-d', FAKE_TEST_DIR,
            '-n', "test_test_test",
            '-i', "file_w_date"
        ])

        res = load(test_args)

        # only two files match type "test_test_test", so expect 2 loads
        self.assertEqual(
            mock_driver_load.call_count,
            2
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

class Test_validate_args(TestCase):

    def test_validate_returns_dict(self):
        from imars_etl.load import _validate_args
        from imars_etl.cli import parse_args

        test_args = parse_args([
            '-vvv',
            'load',
            '--dry_run',
            '-f', "/fake/path/file_w_date_2018.txt",
            '-p', '-1',
            '-t', "2018-02-26T13:00",
            '-j', '{"status_id":1,"area_id":1}',
        ])
        result_arg_dict = _validate_args(test_args)
        self.assertDictContainsSubset(
            {
                "verbose": 3,
                "dry_run": True,
                "filepath": "/fake/path/file_w_date_2018.txt",
                "product_id": -1,
                "product_type_name": "test_test_test",
                "time":  "2018-02-26T13:00",
                "datetime": datetime(2018,2,26,13),
            },
            result_arg_dict
        )

    def test_validate_json_into_args(self):
        """
        validate_args sticks stuff from --json into args
        """
        from imars_etl.load import _validate_args
        from imars_etl.cli import parse_args

        test_args = parse_args([
            '-vvv',
            'load',
            '--dry_run',
            '-f', "/fake/path/file_w_date_2018.txt",
            '-p', '-1',
            '-t', "2018-02-26T13:00",
            '-j', '{"status_id":1,"area_id":1}',
        ])
        result_arg_dict = _validate_args(test_args)
        self.assertDictContainsSubset(
            {
                "verbose": 3,
                "dry_run": True,
                "filepath": "/fake/path/file_w_date_2018.txt",
                "product_id": -1,
                "product_type_name": "test_test_test",
                "date_time":  "2018-02-26T13:00",
                "datetime": datetime(2018,2,26,13),
                "json": '{"status_id":1,"area_id":1}',
                "status_id": 1,
                "area_id": 1
            },
            result_arg_dict
        )

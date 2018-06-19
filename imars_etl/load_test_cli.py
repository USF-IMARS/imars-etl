"""
"""
try:  # py2
    from mock import patch
except ImportError:  # py3
    from unittest.mock import patch


from imars_etl.util.TestCasePlusSQL import TestCasePlusSQL


class Test_load_cli(TestCasePlusSQL):
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
        from imars_etl.cli import main

        test_args = [
            '-vvv',
            'load',
            '--dry_run',
            '-f', "/fake/path/file_w_date_2018.txt",
            '-p', '-1',
            '-t', "2018-02-26T13:00",
            '-j', '{"status_id":1,"area_id":1}',
        ]
        self.assertSQLInsertKeyValuesMatch(
            main(test_args),
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

    def test_load_basic_custom_driver(self):
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
        from imars_etl.cli import main

        FPATH = "/fake/path/file_w_date_2018.txt"

        test_args = [
            '-vvv',
            'load',
            '--dry_run',
            '--storage_driver', 'no_upload',
            '-f', FPATH,
            '-p', '-1',
            '-t', "2018-02-26T13:00",
            '-j', '{"status_id":1,"area_id":1}',
        ]
        self.assertSQLInsertKeyValuesMatch(
            main(test_args),
            ['status_id', 'date_time', 'area_id', 'product_id', 'filepath'],
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
        from imars_etl.cli import main
        test_args = [
            '-vvv',
            'load',
            '--dry_run',
            '-f', "/my/path/without/a/date/in.it",
            '-j', '{"area_id":1}',
            '-p', '6'
        ]
        self.assertRaises(Exception, main, test_args)  # noqa H202

    def test_load_missing_date_guessable(self):
        """
        CLI cmd missing date that *can* be guessed:
            imars_etl.py load
                --dry_run
                -j '{"area_id":1}'
                -p 6
                -f '/path/w/parseable/date/wv2_1989-06-07T1112_myTag.zip'
        """
        from imars_etl.cli import main
        test_args = [
            '-vvv',
            'load',
            '--dry_run',
            '-f', "/path/w/parseable/date/wv2_1989-06-07T1112_myTag.zip",
            '-j', '{"area_id":1}',
            '-p', '6',
        ]
        self.assertSQLInsertKeyValuesMatch(
            main(test_args),
            ['date_time', 'area_id', 'product_id', 'filepath'],
            [
                '"1989-06-07T11:12:00"', '1', '6',
                '"/srv/imars-objects/zip_wv2_ftp_ingest' +
                '/wv2_1989-06-07T1112_myTag.zip"'
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
        from imars_etl.cli import main
        test_args = [
            '-vvv',
            'load',
            '--dry_run',
            '-f', "/path/w/parseable/date/wv2_2000-06-07T1122_myTag.zip",
            '-p', '6',
        ]
        res = main(test_args)
        # 'INSERT INTO file (
        # product_id,filepath,date_time) VALUES (
        # 6,"/srv/imars-objects/zip_wv2_ftp_ingest/wv2_2000-06-07T1122_m...")'
        self.assertSQLInsertKeyValuesMatch(
            res,
            ['product_id', 'filepath', 'date_time'],
            [
                '"/srv/imars-objects/zip_wv2_ftp_ingest' +
                '/wv2_2000-06-07T1122_myTag.zip"',
                '6',
                '"2000-06-07T11:22:00"'
            ]
        )

    def test_load_directory_by_product_id_number(self):
        """
        CLI load directory using product_id
        """
        FAKE_TEST_DIR = "/fake/dir/of/files/w/parseable/dates"
        with patch('os.walk') as mockwalk:
            from imars_etl.cli import main
            mockwalk.return_value = [(
                FAKE_TEST_DIR,  # root
                (  # dirs
                ),
                (  # files
                    "file_w_date_1999.txt",
                    "file_w_date_2018.txt",
                ),
            )]
            test_args = [
                '-vvv',
                'load',
                '--dry_run',
                '-d', FAKE_TEST_DIR,
                '-p', '-1',
                '-i', "file_w_date"
            ]
            self.assertSQLsEquals(
                main(test_args),
                [
                    ['date_time', 'product_id', 'filepath'],
                    ['date_time', 'product_id', 'filepath']
                ],
                [
                    [
                        '"1999-01-01T00:00:00"',
                        '-1',
                        '"/srv/imars-objects/test_test_test' +
                        '/simple_file_with_no_args.txt"'
                    ],
                    [
                        '"2018-01-01T00:00:00"',
                        '-1',
                        '"/srv/imars-objects/test_test_test' +
                        '/simple_file_with_no_args.txt"'
                    ]
                ]
            )

    def test_load_directory_short_name(self):
        """
        CLI load dir using product_type_name (short_name)
        """
        FAKE_TEST_DIR = "/fake/dir/of/files/w/parseable/dates"
        with patch('os.walk') as mockwalk:
            from imars_etl.cli import main
            mockwalk.return_value = [(
                FAKE_TEST_DIR,  # root
                (  # dirs
                ),
                (  # files
                    "file_w_date_1999.txt",
                    "file_w_date_2018.txt",
                ),
            )]
            test_args = [
                '-vvv',
                'load',
                '--dry_run',
                '-d', FAKE_TEST_DIR,
                '-n', "test_test_test",
                '-i', "file_w_date"
            ]
            sql_strs = main(test_args)
            self.assertEqual(len(sql_strs), 2)
            self.assertSQLsEquals(
                sql_strs,
                [
                    ['date_time', 'product_id', 'filepath'],
                    ['date_time', 'product_id', 'filepath']
                ],
                [
                    [
                        '"1999-01-01T00:00:00"', '-1',
                        '"/srv/imars-objects/test_test_test' +
                        '/simple_file_with_no_args.txt"'
                    ],
                    [
                        '"2018-01-01T00:00:00"', '-1',
                        '"/srv/imars-objects/test_test_test' +
                        '/simple_file_with_no_args.txt"'
                    ]
                ]
            )

    def test_custom_input_args_in_json(self):
        """
        imars_etl.load w/ args passed in --json
        """
        from imars_etl.cli import main
        test_args = [
            '-vvv',
            'load',
            '--dry_run',
            '-f', "fake_filepath.bs",
            '-n', "test_fancy_format_test",
            '-i', "file_w_nothing",
            '-t', "2018-01-01T08:08",
            '--json', '{"test_arg":"tssst"}'
        ]
        res = main(test_args)
        self.assertSQLInsertKeyValuesMatch(
            res,
            ['date_time', 'product_id', 'filepath'],
            [
                '"2018-01-01T08:08"',
                '-2',
                '"/srv/imars-objects/_fancy_tssst_/2018-001' +
                '/arg_is_tssst_time_is_0800.fancy_file"'
            ]
        )

    def test_load_directory_only_loads_files_of_given_type(self):
        """
        CLI load dir loads only files that match the given type
        """
        FAKE_TEST_DIR = "/fake/dir/of/files/w/parseable/dates"
        with patch('os.walk') as mockwalk:
            from imars_etl.cli import main
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
                    "my-fake_filename.THAT_does_not-match_anyKnownFormat.EXTEN"
                ),
            )]
            test_args = [
                '-vvv',
                'load',
                '--dry_run',
                '-d', FAKE_TEST_DIR,
                '-n', "test_test_test",
                '-i', "file_w_date"
            ]
            self.assertSQLsEquals(
                main(test_args),
                [
                    ['date_time', 'product_id', 'filepath'],
                    ['date_time', 'product_id', 'filepath']
                ],
                [
                    [
                        '"1999-01-01T00:00:00"',
                        '-1',
                        '"/srv/imars-objects/test_test_test' +
                        '/simple_file_with_no_args.txt"'
                    ],
                    [
                        '"2018-01-01T00:00:00"',
                        '-1',
                        '"/srv/imars-objects/test_test_test' +
                        '/simple_file_with_no_args.txt"'
                    ]
                ]
            )

    @patch('os.walk')
    @patch(
        'imars_etl.Load._actual_load_file_with_driver',
        return_value="/fake/imars-obj/path"
    )
    def test_load_directory_leaves_unmatched_files_alone(
        self, mock_driver_load, mockwalk
    ):
        """
        CLI load dir does not mess with files it cannot identify
        """
        FAKE_TEST_DIR = "/fake/dir/of/files/w/parseable/dates"
        from imars_etl.cli import main

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
        test_args = [
            '-vvv',
            'load',
            '--dry_run',
            '-d', FAKE_TEST_DIR,
            '-n', "test_test_test",
            '-i', "file_w_date"
        ]

        main(test_args)

        # only two files match type "test_test_test", so expect 2 loads
        self.assertEqual(
            mock_driver_load.call_count,
            2
        )

"""
"""
try:  # py2
    import mock
    from mock import patch
except ImportError:  # py3
    from unittest import mock
    from unittest.mock import patch


from imars_etl.util.TestCasePlusSQL import TestCasePlusSQL


class Test_load_cli(TestCasePlusSQL):
    # tests:
    #########################
    # === bash CLI (passes ArgParse objects)
    @patch(
        "imars_etl.object_storage.ObjectStorageHandler."
        "ObjectStorageHandler.load"
    )
    def test_load_basic(self, mock_load):
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
        mock_load.return_value = "/tmp/imars-etl-test-fpath"

        test_args = [
            '-vvv',
            'load',
            '--dry_run',
            '-f', "/fake/path/file_w_date_2018.txt",
            '-p', '-1',
            '-t', "2018-02-26 13:00:00",
            '-j', '{"status_id":1,"area_id":1}',
            '--nohash',
        ]
        self.assertSQLInsertKeyValuesMatch(
            main(test_args),
            ['status_id', 'date_time', 'area_id', 'product_id', 'filepath'],
            [
                '1',
                '"2018-02-26 13:00:00"',
                '1',
                '-1',
                '"{}"'.format(mock_load.return_value)
            ]
        )

    def test_load_basic_custom_obj_store(self):
        """
        CLI using `no_upload` `object_store` passes path through unchanged
            imars_etl.py load
                --dry_run
                --object_store no_upload
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
            '--object_store', 'no_upload',
            '-f', FPATH,
            '-p', '-1',
            '-t', "2018-02-26T13:00",
            '-j', '{"status_id":1,"area_id":1}',
            '--nohash',
        ]
        self.assertSQLInsertKeyValuesMatch(
            main(test_args),
            ['status_id', 'date_time', 'area_id', 'product_id', 'filepath'],
            [
                '1',
                '"2018-02-26 13:00:00"',
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
            '-p', '6',
            '--nohash',
        ]
        self.assertRaises(Exception, main, test_args)  # noqa H202

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
                -f '/path/w/parseable/date/wv2_1989_06_07T111234_gom_123456789_10_0.zip'
        """
        from imars_etl.cli import main
        mock_load.return_value = "/tmp/imars-etl-test-fpath"
        test_args = [
            '-vvv',
            'load',
            '--dry_run',
            '-f', "/path/w/parseable/date/wv2_1989_06_07T111234_gom_123456789_10_0.zip",
            '-j', '{"area_id":1}',
            '-p', '6',
            '--nohash',
        ]
        self.assertSQLInsertKeyValuesMatch(
            main(test_args),
            ['date_time', 'area_id', 'product_id', 'filepath'],
            [
                '"1989-06-07 11:12:34"', '1', '6',
                '"{}"'.format(mock_load.return_value)
            ]
        )

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
                -f '/path/w/parseable/date/wv2_2000_06_gom_123456789_10_0.zip'
        """
        from imars_etl.cli import main
        mock_load.return_value = "/tmp/imars-etl-test-fpath"
        test_args = [
            '-vvv',
            'load',
            '--dry_run',
            '-f', "/path/w/parseable/date/wv2_2000_06_07T112233_gom_123456789_10_0.zip",
            '-p', '6',
            '--nohash',
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

    @patch(
        "imars_etl.object_storage.ObjectStorageHandler."
        "ObjectStorageHandler.load"
    )
    def test_load_directory_by_product_id_number(self, mock_load):
        """
        CLI load directory using product_id
        """
        mock_load.return_value = "/tmp/imars-etl-test-fpath"
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
                '-i', "file_w_date",
                '--nohash',
            ]
            self.assertSQLsEquals(
                main(test_args),
                [
                    ['date_time', 'product_id', 'filepath'],
                    ['date_time', 'product_id', 'filepath']
                ],
                [
                    [
                        '"1999-01-01 00:00:00"',
                        '-1',
                        '"{}"'.format(mock_load.return_value)
                    ],
                    [
                        '"2018-01-01 00:00:00"',
                        '-1',
                        '"{}"'.format(mock_load.return_value)
                    ]
                ]
            )

    @patch(
        "imars_etl.object_storage.ObjectStorageHandler."
        "ObjectStorageHandler.load"
    )
    def test_load_directory_short_name(self, mock_load):
        """
        CLI load dir using product_type_name (short_name)
        """
        mock_load.return_value = "/tmp/imars-etl-test-fpath"
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
                '-i', "file_w_date",
                '--nohash',
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
                        '"1999-01-01 00:00:00"', '-1',
                        '"{}"'.format(mock_load.return_value)
                    ],
                    [
                        '"2018-01-01 00:00:00"', '-1',
                        '"{}"'.format(mock_load.return_value)
                    ]
                ]
            )

    @patch(
        "imars_etl.object_storage.ObjectStorageHandler."
        "ObjectStorageHandler.load",
        return_value="/tmp/imars-etl-test-fpath"
    )
    def test_custom_input_args_in_json(self, mock_load):
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
            '--json', '{"test_arg":"tssst"}',
            '--nohash',
        ]
        res = main(test_args)
        self.assertSQLInsertKeyValuesMatch(
            res,
            ['date_time', 'product_id', 'filepath'],
            [
                '"2018-01-01 08:08:00"',
                '-2',
                '"{}"'.format(mock_load.return_value)
            ]
        )

    @patch(
        "imars_etl.object_storage.ObjectStorageHandler."
        "ObjectStorageHandler.load",
        return_value="/tmp/imars-etl-test-fpath"
    )
    def test_load_directory_only_loads_files_of_given_type(self, mock_load):
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
                '-i', "file_w_date",
                '--nohash',
            ]
            self.assertSQLsEquals(
                main(test_args),
                [
                    ['date_time', 'product_id', 'filepath'],
                    ['date_time', 'product_id', 'filepath']
                ],
                [
                    [
                        '"1999-01-01 00:00:00"',
                        '-1',
                        '"{}"'.format(mock_load.return_value)
                    ],
                    [
                        '"2018-01-01 00:00:00"',
                        '-1',
                        '"{}"'.format(mock_load.return_value)
                    ]
                ]
            )

    @patch('os.walk')
    @patch(
        'imars_etl.object_storage.ObjectStorageHandler.ObjectStorageHandler.load',
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
            '-i', "file_w_date",
            '--nohash',
        ]

        main(test_args)

        # only two files match type "test_test_test", so expect 2 loads
        self.assertEqual(
            mock_driver_load.call_count,
            2
        )

    @patch(
        "imars_etl.object_storage.ObjectStorageHandler."
        "ObjectStorageHandler.load",
        return_value="/tmp/imars-etl-test-fpath"
    )
    def test_load_file_and_metadata_file(
        self, mock_load
    ):
        """
        CLI load file w/ metadata file & json using default (DHUS) parser
        """
        FAKE_UUID = '68ebc577-178e-4d9c-b16a-3bf8f1394939'
        DATETIME = "2018-01-01T08:08:00"
        mocked_open = mock.mock_open(
            read_data='[{"uuid":"' + FAKE_UUID + '"}]'
        )
        with patch(
            'imars_etl.drivers_metadata.dhus_json.open',
            mocked_open, create=True
        ):

            from imars_etl.cli import main

            test_args = [
                '-vvv',
                'load',
                '--dry_run',
                '-m', "/fake/metadata/filepath.json",
                '-f', "/fake/file/path/fake_filepath.bs",
                '-n', "test_fancy_format_test",
                '-i', "file_w_nothing",
                '-t', DATETIME,
                '--json', '{"test_arg":"tssst"}',
                '--nohash',
            ]

            res = main(test_args)
            self.assertSQLInsertKeyValuesMatch(
                res,
                ['date_time', 'product_id', 'uuid', 'filepath'],
                [
                    '"{}"'.format(DATETIME.replace("T", " ")),
                    '-2',
                    '"{}"'.format(FAKE_UUID),
                    '"{}"'.format(mock_load.return_value)
                ]
            )

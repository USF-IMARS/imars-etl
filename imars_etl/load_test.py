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

class Test_load(TestCase):

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
                -j '{"status":1, "area_id":1}'
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
            '-j', '{"status":1,"area_id":1}',
        ])
        self.assertEqual(
            load(test_args),
            'INSERT INTO file'
            + ' (status,date_time,area_id,product_type_id,filepath)'
            + ' VALUES (1,"2018-02-26T13:00",1,-1,"/srv/imars-objects/test_test_test/simple_file_with_no_args.txt")'
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
        self.assertEqual(
            load(test_args),
            'INSERT INTO file'
            + ' (date_time,area_id,product_type_id,filepath)'
            + ' VALUES ("1989-06-07T11:12:00",1,6,'
            + '"/srv/imars-objects/zip_wv2_ftp_ingest/wv2_1989-06-07T1112_myTag.zip")'
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
        self.assertEqual(
            load(test_args),
            'INSERT INTO file'
            + ' (date_time,product_type_id,filepath)'
            + ' VALUES ("2000-06-07T11:22:00",6,'
            + '"/srv/imars-objects/zip_wv2_ftp_ingest/wv2_2000-06-07T1122_myTag.zip")'
        )

    # === python API (passes dicts)
    def test_load_python_basic_dict(self):
        """
        API basic imars_etl.load:
            imars_etl.load({
                "dry_run": True,
                "filepath": "/fake/path/file_w_date_2018.txt",
                "product_type_id": -1,
                "time": "2018-02-26T13:00",
                "json": '{"status":1, "area_id":1}',
                "verbose": 3
            })
        """
        from imars_etl.load import load
        result = load({
            "dry_run": True,
            "filepath": "/fake/path/file_w_date_2018.txt",
            "product_type_id": -1,
            "time": "2018-02-26T13:00",
            "json": '{"status":1, "area_id":1}',
            "verbose": 3
        })
        self.assertEqual(
            result,
            'INSERT INTO file'
            + ' (status,date_time,area_id,product_type_id,filepath)'
            + ' VALUES (1,"2018-02-26T13:00",1,-1,"/srv/imars-objects/test_test_test/simple_file_with_no_args.txt")'
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
            "product_type_id":7,
            # "time":"2016-02-12T16:25:18",
            # "datetime": datetime(2016,2,12,16,25,18),
            "json":'{"status":3,"area_id":5}'
        }
        self.assertEqual(
            load(test_args),
            'INSERT INTO file'
            + ' (status,date_time,area_id,product_type_id,filepath)'
            + ' VALUES (3,"2016-02-12T16:25:18",5,7,'
            + '"/srv/imars-objects/extra_data/WV02/2016.02/WV02_20160212162518_0000000000000000_16Feb12162518-M1BS-057522945010_P002.att")'
        )

    def test_load_directory_by_product_type_id_number(self):
        """
            CLI load directory using product_type_id
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
            self.assertEqual( load(test_args), [
                'INSERT INTO file'
                + ' (date_time,product_type_id,filepath)'
                + ' VALUES ("1999-01-01T00:00:00",-1,'
                + '"/srv/imars-objects/test_test_test/simple_file_with_no_args.txt")',
                'INSERT INTO file'
                + ' (date_time,product_type_id,filepath)'
                + ' VALUES ("2018-01-01T00:00:00",-1,'
                + '"/srv/imars-objects/test_test_test/simple_file_with_no_args.txt")',
            ])

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
            self.assertEqual( load(test_args), [
                'INSERT INTO file'
                + ' (date_time,product_type_id,filepath)'
                + ' VALUES ("1999-01-01T00:00:00",-1,'
                + '"/srv/imars-objects/test_test_test/simple_file_with_no_args.txt")',
                'INSERT INTO file'
                + ' (date_time,product_type_id,filepath)'
                + ' VALUES ("2018-01-01T00:00:00",-1,'
                + '"/srv/imars-objects/test_test_test/simple_file_with_no_args.txt")',
            ])

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
            self.assertEqual( load(test_args), [
                'INSERT INTO file'
                + ' (date_time,product_type_id,filepath)'
                + ' VALUES ("1999-01-01T00:00:00",-1,'
                + '"/srv/imars-objects/test_test_test/simple_file_with_no_args.txt")',
                'INSERT INTO file'
                + ' (date_time,product_type_id,filepath)'
                + ' VALUES ("2018-01-01T00:00:00",-1,'
                + '"/srv/imars-objects/test_test_test/simple_file_with_no_args.txt")',
            ])

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

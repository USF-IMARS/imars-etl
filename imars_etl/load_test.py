"""
"""

# std modules:
from unittest import TestCase
try:
    # py2
    from mock import MagicMock
except ImportError:
    # py3
    from unittest.mock import MagicMock

# dependencies:
from imars_etl.load import load

class Test_load(TestCase):

    # tests:
    #########################
    # === bash CLI (passes ArgParse objects)
    def test_load_basic(self):
        """
        basic load cmd passes:
            imars_etl.py load
                --dry_run
                -f /fake/filepath.png
                -t 4
                -d '2018-02-26T13:00'
                -j '{"status":1, "area_id":1}'
        """
        test_args = MagicMock(
            verbose=0,
            dry_run=True,
            filepath="/fake/filepath.png",
            product_type_id=4,
            date="2018-02-26T13:00",
            json='{"status":1,"area_id":1}'
        )
        self.assertEqual(
            load(test_args),
            'INSERT INTO file'
            + ' (status,date_time,area_id,product_type_id,filepath)'
            + ' VALUES (1,"2018-02-26T13:00",1,4,"/fake/filepath.png")'
        )

    def test_load_missing_date_unguessable(self):
        """
        cmd missing date that cannot be guessed fails:
            imars_etl.py load
                --dry_run
                -t 4
                -j '{"area_id":1}'
                -f '/my/path/without/a/date/in.it'
        """
        test_args = MagicMock(
            verbose=0,
            dry_run=True,
            filepath="/my/path/without/a/date/in.it",
            date=None,
            json='{"area_id":1}',
            product_type_id=4
        )
        self.assertRaises(Exception, load, test_args)

    def test_load_missing_date_guessable(self):
        """
        cmd missing date that *can* be guessed passes:
            imars_etl.py load
                --dry_run
                -j '{"area_id":1}'
                -t 4
                -f '/path/w/parseable/date/wv2_2000_06_myTag.zip'
        """
        test_args = MagicMock(
            verbose=0,
            dry_run=True,
            filepath="/path/w/parseable/date/wv2_2000_06_myTag.zip",
            date=None,
            json='{"area_id":1}',
            product_type_id=4
        )
        self.assertEqual(
            load(test_args),
            'INSERT INTO file'
            + ' (date_time,area_id,product_type_id,filepath)'
            + ' VALUES ("2000-06-01T00:00:00",1,4,'
            + '"/path/w/parseable/date/wv2_2000_06_myTag.zip")'
        )

    def test_wv2_zip_ingest_example(self):
        """
        test wv2 ingest with filename parsing passes:
            imars_etl.py load
                --dry_run
                -t 6
                -f '/path/w/parseable/date/wv2_2000_06_myTag.zip'
        """
        test_args = MagicMock(
            verbose=0,
            dry_run=True,
            filepath="/path/w/parseable/date/wv2_2000_06_myTag.zip",
            date=None,
            product_type_id=6
        )
        self.assertEqual(
            load(test_args),
            'INSERT INTO file'
            + ' (date_time,product_type_id,filepath)'
            + ' VALUES ("2000-06-01T00:00:00",6,'
            + '"/path/w/parseable/date/wv2_2000_06_myTag.zip")'
        )

    # === python API (passes dicts)
    def test_load_python_basic_dict(self):
        """
        basic imars_etl.load passes:
            imars_etl.load({
                "dry_run": True,
                "filepath": "/fake/filepath.png",
                "product_type_id": "4",
                "date": "2018-02-26T13:00",
                "json": '{"status":1, "area_id":1}'
            })
        """
        result = load({
            "dry_run": True,
            "filepath": "/fake/filepath.png",
            "product_type_id": "4",
            "date": "2018-02-26T13:00",
            "json": '{"status":1, "area_id":1}'
        })

        self.assertEqual(
            result,
            'INSERT INTO file'
            + ' (status,date_time,area_id,product_type_id,filepath)'
            + ' VALUES (1,"2018-02-26T13:00",1,4,"/fake/filepath.png")'
        )

    def test_load_att_wv2_m1bs(self):
        """
        load att_wv2_m1bs with inferred date from filepath passes
        """
        test_args = {
            "verbose":3,
            "dry_run":True,
            "filepath":"/tmp/airflow_output_2018-03-01T20:00:00/057522945010_01_003/057522945010_01/057522945010_01_P002_MUL/16FEB12162518-M1BS-057522945010_01_P002.ATT",
            "product_type_id":7,
            # "date":"2016-02-12T16:25:18",
            # "datetime": datetime(2016,2,12,16,25,18),
            "json":'{"status":3,"area_id":5}'
        }
        self.assertEqual(
            load(test_args),
            'INSERT INTO file'
            + ' (status,date_time,area_id,product_type_id,filepath)'
            + ' VALUES (3,"2016-02-12T16:25:18",5,7,'
            + '"/tmp/airflow_output_2018-03-01T20:00:00/057522945010_01_003/057522945010_01/057522945010_01_P002_MUL/16FEB12162518-M1BS-057522945010_01_P002.ATT")'
        )

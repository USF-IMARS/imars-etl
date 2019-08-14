"""
example unit test for ExampleClass
list of unittest assert methods:
https://docs.python.org/3/library/unittest.html#assert-methods
"""

# std modules:
from unittest import TestCase
from unittest.mock import MagicMock

# tested module(s):
from imars_etl.metadata_db.MetadataDBHandler import MetadataDBHandler


class Test_MetadataDBHandler(TestCase):
    def test_get_records_first_returns_list_of_tuples(self):
        """ get_records(first=True) returns obj like [()] """
        handle = MetadataDBHandler()
        handle.get_first = MagicMock(
            name='get_first',
            return_value=("col1_val", 2, "col3_val")
        )
        self.assertEqual(
            handle.get_records("fake sql string", first=True),
            [("col1_val", 2, "col3_val")]
        )

    def test_get_records_first_returns_list_of_tuples_overwrapped(self):
        """ get_records() returns obj like [()] even if get_first misbehaves"""
        handle = MetadataDBHandler()
        handle.get_first = MagicMock(
            name='get_first',
            return_value=[("col1_val", 2, "col3_val")]  # extra unexpected []
        )
        self.assertEqual(
            handle.get_records("fake sql string", first=True),
            [("col1_val", 2, "col3_val")]
        )

    def test_get_records_multiple_returns_list_of_tuples(self):
        """ get_records(first=False) returns obj like [(),()] """
        handle = MetadataDBHandler()
        RECORDS = [
            ("col1_val", 2, "col3_val"),
            ("c1_value", 2, "c3_value")
        ]
        handle._get_records = MagicMock(
            name='_get_records',
            return_value=RECORDS
        )
        self.assertEqual(
            handle.get_records("fake sql string", first=False),
            RECORDS
        )

    def test_get_records_single_returns_list_of_tuples(self):
        """ get_records(first=False) returns obj like [(),()] """
        handle = MetadataDBHandler()
        RECORDS = [
            ("col1_val", 2, "col3_val"),
        ]
        handle._get_records = MagicMock(
            name='_get_records',
            return_value=RECORDS
        )
        self.assertEqual(
            handle.get_records("fake sql string", first=False),
            RECORDS
        )

    def test_get_records_handles_nones(self):
        """ get_records() handles (None) from _get_records """
        handle = MetadataDBHandler()
        RECORDS = [
            (None),
        ]
        handle._get_records = MagicMock(
            name='_get_records',
            return_value=RECORDS
        )
        self.assertEqual(
            handle.get_records("fake sql string", first=False),
            [[]]
        )

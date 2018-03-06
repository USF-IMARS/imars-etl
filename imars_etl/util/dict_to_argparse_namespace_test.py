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
from imars_etl.util import dict_to_argparse_namespace

class Test_dict_to_argparse_namespace(TestCase):

    # tests:
    #########################
    def test_dict_to_argparse_namespace_basic(self):
        """
        test converting a simple dict
        """
        test_dict={
            "a":"apple",
            "beta": "fish",
            "c": "cat"
        }
        args = dict_to_argparse_namespace(test_dict)
        self.assertEqual(args.a,    'apple')
        self.assertEqual(args.beta, 'fish')
        self.assertEqual(args.c,    'cat')

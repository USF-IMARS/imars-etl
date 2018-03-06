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

import argparse

# dependencies:
from imars_etl.util import dict_to_argparse_namespace

class Test_dict_to_argparse_namespace(TestCase):

    # tests:
    #########################
    def test_dict_to_argparse_namespace_basic(self):
        """
        simple dict values accesible w/ attrib refs
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

    def test_returns_namespace_obj(self):
        """
        return type is argparse.Namepace
        """
        test_dict={
            "a":"apple",
            "beta": "fish",
            "c": "cat"
        }
        args = dict_to_argparse_namespace(test_dict)
        self.assertTrue(isinstance(args, argparse.Namespace))

    def test_argparse_passes_through(self):
        """
        passing a argparse.Namespace is fine
        """
        args = argparse.Namespace()
        args.mytest = 'test'
        args2 = dict_to_argparse_namespace(args)
        self.assertEqual(args,args2)

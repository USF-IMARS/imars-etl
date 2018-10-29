"""
"Fake" backend which does nothing.
Attempts to load return sucess without loading anything.
Attempts to extract return empty files.

Useful for testing or for cases when you want to interact with the metadata
only and not actually put/get files.
"""

from imars_etl.object_storage.BaseObjectHook import BaseObjectHook


class NoBackendObjectHook(BaseObjectHook):

    def __init__(self, *args, **kwargs):
        super(NoBackendObjectHook, self).__init__(
            *args, source=None, **kwargs
        )

    def load(self, **kwargs):
        return kwargs['filepath']

    def extract(self, args):
        return ""

    def format_filepath(self, **kwargs):
        print('formatting...')
        print('\n\tgiving back filepath: \t {}\n'.format(kwargs['filepath']))
        return kwargs['filepath']

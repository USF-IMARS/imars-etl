"""
"Fake" backend which does nothing.
Attempts to load return sucess without loading anything.
Attempts to extract return empty files.

Useful for testing or for cases when you want to interact with the metadata
only and not actually put/get files.
"""


class NoBackendStorageDriver():

    def load(self, **kwargs):
        return kwargs['filepath']

    def extract(self, args):
        return ""

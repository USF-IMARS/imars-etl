"""
Base Hook for object storage.
"""


class BaseObjectHook(object):
    def load(self, **kwargs):
        raise NotImplementedError()

    def extract(self, args):
        raise NotImplementedError()

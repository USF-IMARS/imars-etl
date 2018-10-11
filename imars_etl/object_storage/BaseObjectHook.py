"""
Base Hook for object storage.
"""
from airflow.hooks.base_hook import BaseHook


class BaseObjectHook(BaseHook):
    def load(self, **kwargs):
        raise NotImplementedError()

    def extract(self, args):
        raise NotImplementedError()

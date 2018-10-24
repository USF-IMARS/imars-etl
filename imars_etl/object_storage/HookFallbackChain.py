"""
Creates a fallback chain of hooks.
If first hook fails, try #2, if #2 fails, try #3, etc.
"""
from imars_etl.object_storage.BaseObjectHook import BaseObjectHook


class HookFallbackChain(BaseObjectHook):

    def __init__(
        self,
        hooklist=[],
        source=None,
        **kwargs
    ):
        """
        params:
        -------
        hooklist : list of (wrapped) hooks
            eg: [FSHook('/srv/imars-objects'), FSObjectHook()]
        """
        assert len(hooklist) > 0
        self.hooklist = hooklist
        super(HookFallbackChain, self).__init__(
            source, **kwargs
        )

    def load(self, filepath=None, **kwargs):
        exceptions = []
        for hook in self.hooklist:
            try:
                remote_target_path = hook.load(filepath=filepath, **kwargs)
                return remote_target_path
            except Exception as ex:
                exceptions.append(ex)
        else:
            raise Exception(
                "All hooks failed. Exceptions: {}".format(exceptions)
            )

    def extract(self, src_path, target_path, **kwargs):
        exceptions = []
        for hook in self.hooklist:
            try:
                local_path = hook.extract(
                    src_path, target_path, **kwargs
                )
                return local_path
            except Exception as ex:
                exceptions.append(ex)
        else:
            raise Exception(
                "All hooks failed. Exceptions: {}".format(exceptions)
            )

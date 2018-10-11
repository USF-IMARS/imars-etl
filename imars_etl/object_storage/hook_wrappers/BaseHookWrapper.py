"""
Hooks are wrapped to ensure they have the expected methods.
Airflow hooks are a bit fast and loose with standards right now,
so the wrappers bridge gaps so that that (for example)
my_hook_instance.load() loads an object into an object store.

Two hook_types are supported:
    * object_storage : object blob storage accessed by key
    * metadata_db    : object metadata RDB accessed via SQL
"""


class WrapperMismatchException(AttributeError):
    """ raised when wrapper cannot wrap the given hook """


class BaseHookWrapper(object):

    def __init__(self, hook_to_wrap):
        # assert (self.can_wrap(hook_to_wrap))  # should we?
        self.hook = hook_to_wrap

    REQUIRED_ATTRS = {}

    def __getattribute__(self, name):
        """
        Overload getattr to raise WrapperMismatchException when the given
        hook does not contain the attributes required for the wrapper to
        interface properly.
        Without this check wrappers might call methods on incompatible hooks
        with unknown side-effects and difficult-to-debug error messages.
        Yes, we could check types or attrs in init but this is python and duck-
        typers gonna duck-type dammit!
        """
        if name != 'REQUIRED_ATTRS' and name in self.REQUIRED_ATTRS.keys():
            required_attrs = self.REQUIRED_ATTRS[name]
            if all([hasattr(self.hook, attr) for attr in required_attrs]):
                pass
            else:
                raise WrapperMismatchException(
                    "Wrapper cannot apply '{}' to hook '{}'."
                    "Hook does not have all required hook methods for this "
                    "wrapper ({}).".format(name, self.hook, self)
                )
        return object.__getattribute__(self, name)

    # === object_storage methods:
    def load(self, filepath=None, **kwargs):
        raise NotImplementedError()

    def extract(self, src_path, target_path, **kwargs):
        raise NotImplementedError()

    def format_filepath(self):
        raise NotImplementedError()

    # === metadata db methods:
    # TODO ...

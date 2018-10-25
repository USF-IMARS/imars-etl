"""
try_hooks_n_wrappers tries to use each connection hook object in given list
with each wrapper class in given list to accomplish the given method.
args & kwargs for the method are passed as m_args & m_kwargs.

try_hooks_n_wrappers will stop on the first hook that works with any wrapper,
and will stop on the first wrapper that works with a hook,
so the order of these lists can matter.

example:
--------
try_hooks_n_wrappers(
    method='get_first',
    hooks=[MySQLHook(), SQLiteHook()],
    wrappers=[DbAPIHook, JSONFileHook],
    m_args=[sql],
    m_kwargs={}
)

parameters:
-----------
method: string
    method to try using on the wrappers
hooks : list of airflow hooks
    list of hooks to try using (in order)
wrappers : list of wrappers to attempt (in order)
    list of wrappers to attempt using on each hook
m_args : list
    args to pass to method `method(*args)`
m_kwargs : dict
    kwargs to pass to method `method(**kwargs)`
"""


def try_hooks_n_wrappers(
    method,
    hooks,
    wrappers,
    m_args=[],
    m_kwargs={},
):
    for hook in hooks:
        try:  # try this hook
            try:  # directly
                return getattr(hook, method)(*m_args, **m_kwargs)
            except:  # with wrappers
                for wrapper in wrappers:
                    try:  # try this wrapper
                        return getattr(wrapper(hook), method)(
                            *m_args, **m_kwargs
                        )
                    except:  # wrapper did not work
                        continue
        except:  # hook did not work
            continue
    else:
        raise  # none of the hooks worked with any of the wrappers

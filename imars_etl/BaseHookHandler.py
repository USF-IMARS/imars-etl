from imars_etl.get_hook import get_hook_list


class BaseHookHandler(object):
    def __init__(self, hook_conn_id):
        self.hooks_list = get_hook_list(hook_conn_id)

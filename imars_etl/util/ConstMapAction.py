from argparse import Action


class ConstMapAction(Action):
    """
    Takes a dict which maps input strings to arbitrary python objects.
    Think of it like a fancy store_const where the value that gets stored
    is looked up in the dict that is passed in.

    options_map_dict : dict
        mapping of strings input to python objects to be saved on namespace.
    """
    def __init__(self, option_strings, dest, options_map_dict, **kwargs):
        self.options_map_dict = options_map_dict
        super(ConstMapAction, self).__init__(
            option_strings, dest,
            choices=options_map_dict.keys(),
            **kwargs
        )

    def __call__(self, parser, namespace, values, arg_str):
        obj_key = values
        obj_value = self.options_map_dict[obj_key]
        setattr(namespace, self.dest, obj_value)

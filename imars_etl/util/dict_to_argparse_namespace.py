import argparse

def dict_to_argparse_namespace(input_dict):
    """
    converts given argument dict into a proper argparse namespace object
    """
    if isinstance(input_dict, argparse.Namespace):
        return input_dict
    else:
        args = argparse.Namespace()
        for key in input_dict:
            val = input_dict[key]
            setattr(args, key, val)
        return args

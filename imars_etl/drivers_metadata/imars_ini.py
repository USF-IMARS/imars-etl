"""
Methods for reading from ini files created by custom IMaRS scripts.
"""

import configparser


class Parser(object):
    def __init__(self, json_metadata_filepath):
        self.json_metadata_filepath = json_metadata_filepath

    def get_url(ini_filepath):
        """Returns url read from custom ini file"""
        cfg = configparser.ConfigParser()
        cfg.read(ini_filepath)
        try:
            return cfg['myd01']['upstream_download_link']
        except KeyError:
            raise ValueError(
                "could not read upstream_download_link from file '{}'".format(
                    ini_filepath
                )
            )

    def get_uuid():
        raise NotImplementedError("TODO!!!")

"""
Methods for parsing json responses from ESA DHUS.
"""

import json


class Reader(object):
    def __init__(self, json_metadata_filepath):
        self.json_metadata_filepath = json_metadata_filepath

    def get_uuid(self):
        with open(self.json_metadata_filepath) as m_file:
            return json.load(m_file)[0]['uuid']

    def get_url(
        self,
        url_base='https://scihub.copernicus.eu/s3/odata/v1/Products'
    ):
        """Returns url read from DHUS json metadata file"""
        url_fmt_str = url_base + "('{uuid}')/$value"

        return url_fmt_str.format(
            uuid=self.get_uuid()
        )

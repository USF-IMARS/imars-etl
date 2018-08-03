"""
Methods for parsing json responses from ESA DHUS.
"""

import json
from datetime import datetime


class Parser(object):
    def __init__(self, json_metadata_filepath):
        self.json_metadata_filepath = json_metadata_filepath

    def get_metadata(self):
        return {
            'uuid': self.get_uuid(),
            'date_time': self.get_datetime(),
        }

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

    def get_datetime(self):
        try:
            with open(self.json_metadata_filepath) as m_file:
                data = json.load(m_file)[0]
                for ind in data['indexes']:
                    if ind['name'] == "product":
                        for chil in ind['children']:
                            if chil['name'] == 'Sensing start':
                                return datetime.strptime(
                                    chil['value'],
                                    "%Y-%m-%dT%H:%M:%S.%fZ"
                                )
        except KeyError:
            return None

"""
Methods for parsing metadata xml file from digital globe wv2 files.
"""
import logging
from xml.etree import ElementTree


class Parser(object):
    def __init__(self, metadata_filepath):
        self.logger = logging.getLogger(__name__)

        self.metadata_filepath = metadata_filepath
        self.xml_root = ElementTree.parse(metadata_filepath).getroot()
        self.logger.debug('init')

    def get_metadata(self):
        self.logger.debug('get_meta')
        return {
            "time": self.get_tlc_time(),
        }

    def get_tlc_time(self):
        tstr = self.xml_root.find('IMD').find('IMAGE').find('TLCTIME').text
        tstr = tstr.replace("Z", "")  # rm UTC timezone @ end
        tstr = tstr.strip()
        self.logger.debug('tstr:{}'.format(tstr))
        return tstr

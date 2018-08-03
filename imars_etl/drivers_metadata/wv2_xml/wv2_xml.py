"""
Methods for parsing metadata xml file from digital globe wv2 files.
"""
from xml.etree import ElementTree


class Parser(object):
    def __init__(self, metadata_filepath):
        self.metadata_filepath = metadata_filepath
        self.xml_root = ElementTree.parse(metadata_filepath).getroot()
        print('init')

    def get_metadata(self):
        print('get_meta')
        return {
            "time": self.get_tlc_time(),
        }

    def get_tlc_time(self):
        tstr = self.xml_root.find('IMD').find('IMAGE').find('TLCTIME').text
        tstr = tstr.replace("Z", "")  # rm UTC timezone @ end
        tstr = tstr.strip()
        print('tstr:{}'.format(tstr))
        return tstr

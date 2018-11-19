from unittest import TestCase

from imars_etl.drivers_metadata.wv2_xml import wv2_xml


class Test_wv2_xml_parser(TestCase):

    # tests:
    #########################
    def test_parse_sample_file(self):
        """
        parse simple example xml file
        """
        parser = wv2_xml.Parser(
            'imars_etl/drivers_metadata/wv2_xml/test_files/'
            'WV02_20140218163417_103001002E0EB600_14FEB18163417-M1BS-'
            '500534956040_01_P004.xml'
        )
        metadata_read = parser.get_metadata()
        print('md: {}'.format(metadata_read))
        expected_subset = {
                "time": "2014-02-18T16:34:17.926650"
        }

        assert expected_subset.items() <= metadata_read.items()

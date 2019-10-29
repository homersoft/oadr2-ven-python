from lxml import etree


def read_xml(path):
    utf8_parser = etree.XMLParser(encoding="utf-8")
    with open(path) as xml_file:
        return etree.fromstring(xml_file.read().encode("utf-8"), parser=utf8_parser)

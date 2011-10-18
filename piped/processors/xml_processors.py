import re

from zope import interface

from piped import exceptions, processing, util
from piped.processors import base


try:
    from lxml import etree
except ImportError:
    etree = None


class MarkupRemover(base.MappingProcessor):
    """ Removes all markup from the text at the provided paths.
    """
    interface.classProvides(processing.IProcessor)
    name = 'remove-markup'

    regex = re.compile(r'<[^>]+>')

    def process_mapping(self, input, *a, **kw):
        return self.regex.sub('', input).strip()


class XPathProcessor(base.Processor):
    """ Parses the XML provided at *xml_path*, and processes the
    *xpaths* defined. """
    interface.classProvides(processing.IProcessor)
    name = 'process-xpaths'

    def __init__(self, xml_path, xpaths, **kw):
        """
        :param xml_path: The path where the XML is found.

        :param xpaths: a dictionary that maps XPath-expressions to an
            output path. The output-path, which is assumed to be empty
            or a list, is extended with the results of the
            XPath-expression.
        """
        self._fail_if_lxml_is_not_available()
        super(XPathProcessor, self).__init__(**kw)
        self.xml_path = xml_path
        self.xpaths = xpaths

    def _fail_if_lxml_is_not_available(self):
        if not etree:
            e_msg = 'missing dependency: lxml'
            detail = 'You must install the "lxml"-library to use the XPath-processor'
            raise exceptions.ConfigurationError(e_msg, detail)

    def process(self, baton):
        xml = util.dict_get_path(baton, self.xml_path)
        e = etree.XML(xml)

        for xpath, output_path in self.xpaths.items():
            result = e.xpath(xpath)
            util.dict_setdefault_path(baton, output_path, []).extend(result)

        return baton

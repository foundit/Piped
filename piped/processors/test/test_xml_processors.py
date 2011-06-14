# Copyright (c) 2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
from twisted.internet import defer
from twisted.trial import unittest

from piped.processors import xml_processors


class MarkupRemoverTest(unittest.TestCase):

    @defer.inlineCallbacks
    def test_simple_removal(self):
        xml = """
            <tag attribute="value">
                contents
            </tag>
        """
        processor = xml_processors.MarkupRemover(mapping=[''])
        baton = yield processor.process(xml)
        self.assertEquals(baton, 'contents')


class XPathProcessorTest(unittest.TestCase):

    def test_processing_xpaths(self):
        xpaths = {
            '//a/b/@c': 'foo',
            '//b/text()': 'bar',
        }
        processor = xml_processors.XPathProcessor('xml', xpaths=xpaths)

        baton = dict(xml='<document><a><b c="c1">b1</b></a><a><b c="c2">b2</b></a></document>')
        processor.process(baton)

        self.assertEquals(baton['foo'], ['c1', 'c2'])
        self.assertEquals(baton['bar'], ['b1', 'b2'])


try:
    import lxml
except ImportError:
    XPathProcessorTest.skip = 'lxml not available'

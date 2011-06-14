# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
from twisted.python import filepath
from twisted.trial import unittest
import yaml

# this is imported in order to register the custom constructors and representers:
from piped import yamlutil


class TestLoadDumpConfiguration(unittest.TestCase):

    def test_filepath_loading_and_saving(self):
        expected_config = dict(
            foo=filepath.FilePath('/foo'),
            bar=filepath.FilePath('/foo/bar'),

            # make sure we dont specify our compiled file here:
            current_module=filepath.FilePath(__file__.replace('.pyc', '.py'))
        )

        saved_config = """
            foo: !filepath /foo
            bar: !filepath /foo/bar
            current_module: !pipedpath test/test_yamlutil.py
        """

        config = yaml.load(saved_config)

        self.assertEquals(config, expected_config)

        def _only_lines(string):
            return [line for line in string.split('\n') if line]

        expected_saved_config = [
            "bar: !filepath '/foo/bar'",
            "current_module: !pipedpath 'test/test_yamlutil.py'",
            "foo: !filepath '/foo'",
        ]

        saved_config = yaml.dump(config, default_flow_style=False)

        self.assertEquals(sorted(_only_lines(saved_config)), expected_saved_config)

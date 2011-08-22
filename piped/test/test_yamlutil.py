# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
from twisted.python import filepath
from twisted.trial import unittest
import yaml
from yaml import constructor

# this is imported in order to register the custom constructors and representers:
from piped import yamlutil


class TestLoadDumpConfiguration(unittest.TestCase):

    def _get_nonblank_lines(self, string):
        return [line for line in string.split('\n') if line]

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

        expected_saved_config = [
            "bar: !filepath '/foo/bar'",
            "current_module: !pipedpath 'test/test_yamlutil.py'",
            "foo: !filepath '/foo'",
        ]

        saved_config = yaml.dump(config, default_flow_style=False)

        self.assertEquals(sorted(self._get_nonblank_lines(saved_config)), expected_saved_config)

    def test_baton_path_loading_and_dumping(self):
        saved_config = """
            foo: !path foo_path
            bar: !path another path with a space
            baz: nothing special
        """

        config = yaml.load(saved_config)

        expected_config = dict(
            foo = yamlutil.BatonPath('foo_path'),
            bar = yamlutil.BatonPath('another path with a space'),
            baz = 'nothing special'
        )

        self.assertEquals(config, expected_config)

        saved_config = yaml.dump(config, default_flow_style=False)
        expected_saved_config = [
            "bar: !path 'another path with a space'",
            "baz: nothing special",
            "foo: !path 'foo_path'",
        ]

        self.assertEquals(sorted(self._get_nonblank_lines(saved_config)), expected_saved_config)

    def test_invalid_baton_path(self):
        saved_config = """
            foo: !path
                nested: 123
        """

        # the !path should be a scalar
        self.assertRaises(constructor.ConstructorError, yaml.load, saved_config)
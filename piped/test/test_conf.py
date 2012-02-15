# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.

import os
import tempfile

from twisted.python import filepath
from twisted.trial import unittest

from piped import conf, exceptions


class ConfigurationManagerTest(unittest.TestCase):

    def setUp(self):
        self.cm = conf.ConfigurationManager()

    def test_get_with_path(self):
        config = dict(foo=123, bar=dict(baz='value'))
        self.cm._config = config
        self.assertEquals(self.cm.get('foo'), 123)
        self.assertEquals(self.cm.get('bar'), dict(baz='value'))
        self.assertEquals(self.cm.get('bar.baz'), 'value')
        self.assertEquals(self.cm.get('nonexistant', 'fallback'), 'fallback')
        self.assertEquals(self.cm.get(('bar', 'baz'), 'fallback'), 'value')

    def test_set_with_path(self):
        config = dict(foo=123, bar=dict(baz='value'))
        self.cm._config = config

        self.cm.set('foo', 42)
        self.assertEquals(self.cm.get('foo'), 42)
        self.cm.set('bar.baz', 'another value')
        self.assertEquals(self.cm.get('bar'), dict(baz='another value'))
        self.assertEquals(self.cm.get('bar.baz'), 'another value')

        self.cm.set('a.b', 'c')
        self.assertEquals(self.cm.get('a'), dict(b='c'))
        self.assertEquals(self.cm.get('a.b'), 'c')
        self.assertEquals(self.cm.get(''), dict(foo=42, bar=dict(baz='another value'),
                                               a=dict(b='c')))

        self.assertEquals(self.cm.setdefault('a.b', 'not c'), 'c')
        self.assertEquals(self.cm.setdefault('a.d', 'not c'), 'not c')

    def test_loading_nonexistent(self):
        nonexistent_file = filepath.FilePath(__file__).sibling('nonexistent')
        self.assertRaises(exceptions.ConfigurationError, self.cm.load_from_file, nonexistent_file)

    def test_loading(self):
        test_config_file = filepath.FilePath(__file__).parent().preauthChild(os.path.join('data', 'test_conf.yml'))
        self.cm.load_from_file(test_config_file)

        # this is the contents of the test_conf.yml file
        self.assertEquals(self.cm.get(''), {'root_level': {'nested': {'another_leaf': 'abc', 'leaf': 123}}})

    def test_loading_invalid(self):
        test_invalid_config_file = filepath.FilePath(__file__).parent().preauthChild(os.path.join('data', 'test_invalid_conf.yml'))
        self.assertRaises(exceptions.ConfigurationError, self.cm.load_from_file, test_invalid_config_file)

    def test_loading_includes(self):
        test_includes_file = filepath.FilePath(__file__).parent().preauthChild(os.path.join('data', 'test_includes.yml'))
        self.cm.load_from_file(test_includes_file)

        # this should include the test_conf.yml file and override some elements
        expected_config = dict(
            added = 123,
            nested = dict(
                leaf = 123,
                another_leaf = 'overridden'
            )
        )

        self.assertEquals(self.cm.get('root_level'), expected_config)

    def test_loading_nested_includes(self):
        test_includes_file = filepath.FilePath(__file__).parent().preauthChild(os.path.join('data', 'test_config_nesting.yml'))
        self.cm.load_from_file(test_includes_file)

        # this should include the test_conf.yml under "foo"
        expected_config = dict(
            bar = dict(
                root_level = dict(
                    added = 42,
                    nested = dict(
                        leaf = 123,
                        another_leaf = 'overridden'
                    )
                )
            ),
            baz = 93
        )

        self.assertEquals(self.cm.get('foo'), expected_config)

    def test_loading_nonexistant_include_fails(self):
        fp = filepath.FilePath(__file__).child('does not exist')
        self.assertRaises(exceptions.ConfigurationError, self.cm._load_includes, dict(), [fp], list())

    def test_empty_config(self):
        self.assertEquals(self.cm._load_config(None, ['a_file']), dict())

    def test_non_dict_config(self):
        self.assertRaises(exceptions.ConfigurationError, self.cm._load_config, [1,2,3], ['a_file'])


class TestAliases(unittest.TestCase):

    def setUp(self):
        self.cm = conf.ConfigurationManager()

    def load_config_from_string(self, string):
        tmp = tempfile.NamedTemporaryFile(suffix='piped-test.conf')

        tmp.file.write(string)
        tmp.file.flush()

        self.cm.load_from_file(tmp.name)
        tmp.file.close()

    def test_simple_alias(self):
        config_string = """
            foo:
                bar: 123
            another: !alias:foo
        """
        self.load_config_from_string(config_string)

        self.assertEquals(self.cm.get('foo'), self.cm.get('another'))

    def test_partial_alias(self):
        config_string = """
            foo:
                bar: 123
            another: !alias:foo
                default: 42
        """
        self.load_config_from_string(config_string)

        self.assertEquals(self.cm.get('another'), dict(bar=123, default=42))

    def test_nested_alises(self):
        config_string = """
            foo:
               bar:
                  baz:
                    qux: 123
            another: !alias:foo
            bar: !alias:foo.bar
        """
        self.load_config_from_string(config_string)

        self.assertEquals(self.cm.get('foo'), self.cm.get('another'))
        self.assertEquals(self.cm.get('foo.bar.baz.qux'), 123)
        self.assertEquals(self.cm.get('another.bar.baz.qux'), 123)
        self.assertEquals(self.cm.get('bar.baz.qux'), 123)

    def test_recursiveness(self):
        config_string = """
            foo:
               bar:
                  baz:
                    qux: 123
                    zip: !alias:foo
        """
        self.load_config_from_string(config_string)

        self.assertEquals(self.cm.get('foo.bar.baz.zip'), self.cm.get('foo'))
        self.assertEquals(self.cm.get('foo.bar.baz.zip.bar.baz.qux'), 123)
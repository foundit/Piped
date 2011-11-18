# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
from twisted.python import failure
from twisted.trial import unittest

from piped import debugger, util


class TestDebugger(unittest.TestCase):
    def setUp(self):
        def raiser():
            self # just referencing it, so it shows up in the namespace

            foo = 414
            raise Exception('this is a raised exception')
        try:
            raiser()
        except:
            f = util.NonCleaningFailure()
        self.debugger = debugger.Debugger(f)

    def assertDebuggerHistoryEndsWith(self, should_end_with):
        self.assertEquals(self.debugger.get_history()[-len(should_end_with):], should_end_with)

    def test_history_simple(self):
        should_end_with = """, in raiser\n    raise Exception(\'this is a raised exception\')\nexceptions.Exception: this is a raised exception\n"""
        self.assertDebuggerHistoryEndsWith(should_end_with)

        result = self.debugger.exec_expr('print "hello world"')
        self.assertEquals(result, 'hello world\n')

        result = self.debugger.exec_expr('print 1+2*3')
        self.assertEquals(result, '7\n')

        should_end_with = """>>> print "hello world"\n"""+\
                          """hello world\n"""+\
                          """>>> print 1+2*3\n"""+\
                          """7\n"""
        self.assertDebuggerHistoryEndsWith(should_end_with)


    def test_history_raise(self):
        self.debugger.exec_expr('raise Exception("raised from the debugger")')
        should_end_with = """Exception: raised from the debugger\n"""
        self.assertDebuggerHistoryEndsWith(should_end_with)

    def test_debugger_namespace(self):
        result = self.debugger.exec_expr('print foo, type(foo)')
        self.assertEqual(result, "414 <type 'int'>\n")

        self.assertEqual(self.debugger.exec_expr('id(self)'), str(id(self))+'\n')

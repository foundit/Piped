# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import datetime
from StringIO import StringIO

from piped import evalcontext


# TODO: Docs
class Debugger(object):

    def __init__(self, failure):
        self.last_command = datetime.datetime.now()
        self.contexts = list()
        self.failure = failure
        for frame in self.failure.frames:
            locals = dict(frame[-2])
            globals = dict(frame[-1])
            context = evalcontext.EvalContext(locals, globals)
            self.contexts.append(context)
        self.buffer = StringIO()
        self.failure.printTraceback(self.buffer, elideFrameworkCode=True)

    def exec_expr(self, expr, frame_no=-1):
        self.last_command = datetime.datetime.now()
        result = self.contexts[frame_no].exec_expr(expr)
        self.buffer.write('>>> '+expr+'\n')
        self.buffer.write(result)
        if not result.endswith('\n'):
            self.buffer.write('\n')
        return result

    def get_history(self):
        return self.buffer.getvalue()


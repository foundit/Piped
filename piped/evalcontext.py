# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
#
# This file is adapted from WebError (MIT-Licensed), but significantly
# modified in order to avoid having to perform any tracing
import sys
import threading
import traceback
from cStringIO import StringIO


exec_lock = threading.Lock()


class EvalContext(object):
    """
    Class that represents a interactive interface.  It has its own
    namespace.  Use eval_context.exec_expr(expr) to run commands; the
    output of those commands is returned, as are print statements.

    This is essentially what doctest does, and is taken directly from
    doctest.
    """

    def __init__(self, namespace, globs):
        self.namespace = namespace
        self.globs = globs

    def exec_expr(self, s):
        out = StringIO()
        sys_stdout = sys.stdout
        with exec_lock:
            sys.stdout = out
            try:
                code = compile(s, '<debug>', "single", 0, 1)
                exec code in self.namespace, self.globs
            except KeyboardInterrupt:
                raise
            except:
                traceback.print_exc(file=out)
            finally:
                sys.stdout = sys_stdout
        return out.getvalue()

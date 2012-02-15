import os
import threading
import zookeeper
import sys
from twisted.internet import reactor

from piped import log


_installed = False
_relay_thread = None
_logging_pipe = None


def _relay_log():
    global _installed, _logging_pipe
    r, w = _logging_pipe
    f = os.fdopen(r)

    levels = dict(
        ZOO_INFO = 'info',
        ZOO_WARN = 'warn',
        ZOO_ERROR = 'error',
        ZOO_DEBUG = 'debug',
    )

    # this function is used as a small redirect in order to make sure that this module
    # is considered the "calling module" by piped.log, and not twisted.internet.base, which
    # is used if reactor.callFromThread is allowed to log directly.
    def log_message(logger, message):
        logger(message)

    while _installed:
        try:
            line = f.readline().strip()

            if '@' in line:
                level, message = line.split('@', 1)
                level = levels.get(level.split(':')[-1])

                # this line is definitely misclassified in the C client....
                if 'Exceeded deadline by' in line and level == 'warn':
                    level = 'debug'

                # reclassify failed server connection attemps as INFO instead of ERROR:
                if 'server refused to accept the client' in line and level == 'error':
                    level = 'info'

            else:
                level = None
                message = line # TODO: can we genereate a better logging message?

            if level is None:
                reactor.callFromThread(log_message, log.info, message)
            else:
                reactor.callFromThread(log_message, getattr(log, level), message)
        except Exception as v:
            log.error('Exception occurred while relaying zookeeper log: [{0}]'.format(v))


def is_installed():
    return _installed

def install():
    global _installed, _relay_thread, _logging_pipe

    if is_installed():
        return

    _logging_pipe = os.pipe()

    zookeeper.set_log_stream(os.fdopen(_logging_pipe[1], 'w'))

    _installed = True

    _relay_thread = threading.Thread(target=_relay_log)
    _relay_thread.setDaemon(True) # die along with the interpreter
    _relay_thread.start()

def uninstall():
    if not is_installed():
        return

    global _installed, _relay_thread

    _installed = False
    zookeeper.set_log_stream(sys.stderr)
    # TODO: make sure the thread is actually stopped

    _relay_thread.join()
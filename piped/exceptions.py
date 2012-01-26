# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import re
import sys

from twisted.spread import pb, jelly, flavors

from piped import util


def unwrap_wrapped_text(text):
    """ Unwraps multi-line strings, but keeps paragraphs separated by
    two or more newlines separated by newlines.
    """
    result = []
    _re_indentation = re.compile(r'^[ \t]*|[ \t]*$', re.MULTILINE)
    for paragraph in re.split(r'\n{2,}', text):
        paragraph = _re_indentation.sub('', paragraph)
        result.append(re.sub(r'\n', ' ', paragraph))
    return '\n'.join(result)


class PipedError(Exception):
    """ Base class for exceptions raised by our code.

    All errors must have a primary error message *msg*. This should
    be a short and sweet description of the problem.

    Any exceptions that can be exposed to users should also have a
    *detail* message, and if appropriate, a *hint* message that
    explains how to solve the problem. Mind that developers using a
    public API are also considered "users" in this regard.

    Please see the error message guidelines.

    :todo: Write error message guidelines and provide a
        cross-reference. :-)
    """
    kind = u'ERROR'

    def __init__(self, msg, detail=None, hint=None):
        self.msg = msg
        self.detail = detail
        self.hint = hint
        self.args = (msg, detail, hint)

    @classmethod
    def _cleanup(cls, text):
        # So we want the messages to be Unicode. However, if that
        # fails, we can't fail here with a UnicodeError, as that would
        # mask the real error --- thus errors='replace'.
        #
        # We unwrap the text, since we're often provided with wrapped
        # multi line strings.
        return util.ensure_unicode(unwrap_wrapped_text(text), errors='replace')

    def __unicode__(self):
        # We prefix even the primary message with a newline and ERROR,
        # so it's easier to spot the short message in a huge-ish
        # traceback.
        result = [u'\n%s: %s' % (self.kind, self._cleanup(self.msg))]
        if self.detail:
            result.append(u'DETAIL: ' + self._cleanup(self.detail))
        if self.hint:
            result.append(u'HINT: ' + self._cleanup(self.hint))
        return u'\n'.join(result)

    def __str__(self):
        return self.__unicode__().encode('utf8')

    def __repr__(self):
        return (u"%s: %s" % (type(self).__name__, self.msg)).encode('utf8')


class PipedWarning(PipedError, UserWarning):
    kind = u'WARNING'


class MissingCallback(PipedError):
    """ Raised when pipeline processing does not result in a callback/errback. """


class ForcedError(PipedError):
    """ Used when we are forcing an exception to be raised.

    Used in unit tests.
    """


class UnsafeThreadingError(PipedError):
    """ Used when we are using possible dangerous threading without specifying
        that we really want to. """


class ValidatorError(PipedError):
    """ Used when the validator-class-decorator detects invalid values. """


class ConfigurationError(PipedError):
    """ An exception related to the configuration. """


class ConfigurationWarning(PipedWarning):
    """ A warning related to the configuration. """


class ConfigurationNotLoaded(ConfigurationError):
    """ Raised when the configuration is used before being loaded. """


class ConfigurationAlreadyLoaded(ConfigurationError):
    """ Raised when the configuration is loaded twice and no reload is specified. """


class InvalidConfigurationError(ConfigurationError):
    """ Raised when an invalid configuratin causes an error that can be
    remedied by fixing the configuration."""


class ProcessorGraphError(PipedError):
    """ Raised when a pipeline is not well-defined and -configured. """


class InvalidPipelineError(PipedError):
    """ Raise when the provided pipeline does not exist. """


class PluginError(PipedError):
    """ Plugin-related errors. """


class ResourceError(PipedError):
    """ Base class for exceptions in the resource system. """


class ReplayedLost(ResourceError):
    """ Used when an on_lost is replayed to a consumer in order to avoid
    having to store the arguments of the previous on_lost call
    """

class InitiallyLost(ResourceError):
    """ Raised when an ResourceDependency is resolved for the first time,
    ensuring any consumer are notified that this resource is currently
    unprovided.
    """

class ProviderConflict(ResourceError):
    """ Raised when a resource is attempted provided by multiple
    providers. """


class UnprovidedResourceError(ResourceError):
    """ Raised when a requested resource cannot be satisfied. """


class UnsatisfiedDependencyError(ResourceError):
    """ Raised when an unsatisfied dependency is attempted used. """


class CircularDependencyGraph(ResourceError):
    """ Raised when adding a dependency to the dependency graph would
    cause the graph to contain a cycle, which is unresolvable. """


class AlreadyExistingDependency(ResourceError):
    """ Raised when an already existing dependency is being added. """


class KeyAlreadyExists(ResourceError):
    """ Raised when a `.dependency.DependencyMap` receives multiple dependencies with
    the same key. """


class AllPipelinesFailedError(PipedError):
    """ Raised when a result from any pipeline is expected, but all failed.

    :ivar failures: List of failures.
    """

    def __init__(self, e_msg, failures, **kw):
        super(AllPipelinesFailedError, self).__init__(e_msg, **kw)
        self.failures = failures


class TimeoutError(PipedError):
    """ Something timed out. """

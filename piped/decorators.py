# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.

""" Decorators used in the project """


def coroutine(func):
    """ Advances the coroutine to its first yield. """

    def wrapper(*args, **kw):
        gen = func(*args, **kw)
        gen.next()
        return gen

    wrapper.__name__ = func.__name__
    wrapper.__dict__ = func.__dict__
    wrapper.__doc__ = func.__doc__
    return wrapper

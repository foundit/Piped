# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.

""" Processors that deal with external processes. """
import subprocess

from twisted.internet import defer, threads
from zope import interface

from piped import exceptions, processing
from piped.processors import base


class RenderDot(base.InputOutputProcessor):
    """ Renders a dot graph. """
    name = 'render-dot'
    interface.classProvides(processing.IProcessor)

    def __init__(self, type='png', **kw):
        kw.setdefault('input_path', 'dot')
        super(RenderDot, self).__init__(**kw)

        self.type = type

    @defer.inlineCallbacks
    def process_input(self, input, baton):
        output, stderr = yield threads.deferToThread(self._invoke_dot_in_subprocess, input, type=self.type)
        if stderr:
            e_msg = 'Error while using dot.'
            raise exceptions.PipedError(e_msg, detail=stderr)
        defer.returnValue(output)

    def _invoke_dot_in_subprocess(self, dot_data, type):
        process = subprocess.Popen(['dot', '-T%s' % type], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        stdout, stderr = process.communicate(dot_data)
        return stdout, stderr


class IOStatParser(base.InputOutputProcessor):
    """ Parses output from iostat.

    Format is a list of 2-tuples where the first element is the name of the device and the
    second element is a list of measurement names that are be used to parse the input into
    a dict.

    .. highlight:: yaml
    
    Example format::

        format:
          - - disk0
            - - KB/t
              - tps
              - MB/s
          # alternative yaml layouts with equivalent structure:
          - [cpu, [user, system, idle]]
          - - load
            - [1m, 5m, 15m]

    The above format can be used to parse the following output::

        '   24.00   2  0.05   3  3 94  0.61 0.69 0.79'

    into::

        {'cpu': {'idle': 94.0, 'system': 3.0, 'user': 3.0},
         'disk0': {'KB/t': 24.00, 'MB/s': 0.05, 'tps': 2.0},
         'load': {'15m': 0.79, '1m': 0.61, '5m': 0.69}}
    """
    interface.classProvides(processing.IProcessor)
    name = 'parse-iostat-output'

    default_format = [
        ('disk0', ('KB/t', 'tps', 'MB/s'))
    ]

    def __init__(self, format=None, **kw):
        kw.setdefault('input_path', 'line')
        super(IOStatParser, self).__init__(**kw)

        self.format = self.default_format
        if format is not None:
            self.format = format

    def process_input(self, input, baton):
        result = dict()
        stat_lines = input.strip().split('\n') # handle multiple lines
        for stat_line in stat_lines:
            stats = stat_line.split()
            for device_name, stat_keys in self.format:
                for stat_key in stat_keys:
                    stat = float(stats.pop(0)) # output from iostat are always floats or ints
                    if len(stat_lines) == 1: # if we only have one measurement, set them on the dictionary
                        result.setdefault(device_name, dict())[stat_key] = stat
                    else: # otherwise, append them to a list of measurements
                        result.setdefault(device_name, dict()).setdefault(stat_key, list()).append(stat)
        return result

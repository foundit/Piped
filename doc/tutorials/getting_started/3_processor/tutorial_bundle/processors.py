from zope import interface

from piped import processing
from piped.processors import base


class Uppercaser(base.InputOutputProcessor):
    # the following line states that this class is an IProcessor, and makes instances of
    # it available for use in the pipelines:
    interface.classProvides(processing.IProcessor)

    # every processor needs a name, which is used in the configuration:
    name = 'uppercase'

    def process_input(self, input, baton):
        return input.upper()
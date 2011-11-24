# encoding: utf8
# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
import difflib
import pprint
import warnings

from mock import patch
from twisted.internet import reactor, defer
from twisted.trial import unittest
from twisted.python import failure, reflect
from zope import interface

from piped import exceptions, processing, util
from piped.processors import base, util_processors, pipeline_processors


class StubProcessorGraphFactory(processing.ProcessorGraphFactory):
    pipeline_configuration_path = 'stub'


class StubProcessor(base.Processor):
    @property
    def name(self):
        return self.__class__.__name__

    def process(self, baton):
        pass


class StubException(Exception):
    pass


class StubPluginManager(object):

    def __init__(self):
        self.plugins = set([ReversingProcessor, UppercasingProcessor, LowercasingProcessor, ExceptionRaisingProcessor])
        self.plugin_by_name = {}
        self._providers_by_keyword = dict()
        for plugin_class in self.plugins:
            self.plugin_by_name[plugin_class.name] = plugin_class

        self.get_plugin_factory = self.plugin_by_name.get

    def get_providers_of_keyword(self, keyword):
        return self._providers_by_keyword.get(keyword, tuple())


class ExceptionRaisingProcessor(StubProcessor):
    """ Simple processor that raises an Exception. """
    name = "raise-exception"

    def process(self, baton):
        raise StubException()


class ReversingProcessor(StubProcessor):
    """ Simple processor that reverses the input. """
    name = "reverse"

    def process(self, baton):
        return baton[::-1]


class UppercasingProcessor(StubProcessor):
    """ Simple processor that uppercases the input. """
    name = "uppercase"

    def process(self, baton):
        return baton.upper()


class LowercasingProcessor(StubProcessor):
    """ Simple processor that uppercases the input. """
    name = "lowercase"

    def process(self, baton):
        return baton.lower()


class CallbackingLaterProcessor(StubProcessor):
    """ Simple processor that callbacks a deferred in the next mainloop iteration. """
    name = "callback-later"

    def process(self, baton):
        d = defer.Deferred()
        reactor.callLater(0, d.callback, baton)
        return d


class ListAppendingProcessor(StubProcessor):
    name = 'list-appender'

    def __init__(self, list_, **kw):
        super(ListAppendingProcessor, self).__init__(**kw)
        self.list = list_

    def process(self, baton):
        self.list.append(baton)
        return baton


class IncrementingProcessor(StubProcessor):
    def process(self, baton):
        baton['n'] += 1
        return baton


class DelayedErrbackProcessor(StubProcessor):
    def __init__(self, delay=0, *a, **kw):
        super(DelayedErrbackProcessor, self).__init__(*a, **kw)
        self.delay = delay

    @defer.inlineCallbacks
    def process(self, baton):
        yield util.wait(self.delay)
        raise StubException


class ProcessorGraphTest(unittest.TestCase):

    def setUp(self):
        self.plugin_manager = StubPluginManager()

    def get_processor_graph_factory(self, pipelines_configuration):
        pgf = processing.ProcessorGraphFactory()
        runtime_environment = processing.RuntimeEnvironment()
        runtime_environment.configuration_manager.set('pipelines', pipelines_configuration)
        pgf.configure(runtime_environment)
        pgf.plugin_manager = self.plugin_manager
        return pgf


class TestProcessorGraphFactory(ProcessorGraphTest):

    def assertConfigurationProperlyTransformed(self, pipeline_configuration,
                                               expected_pipeline_configuration, msg=''):
        pgf = self.get_processor_graph_factory(pipeline_configuration)
        self.assertFalse(pgf.pipelines_configuration is pipeline_configuration, msg)
        self.assertEquals(pgf.pipelines_configuration, expected_pipeline_configuration, msg)

    def get_processor_definitions(self, processor_name, **processor_kwargs):
        """ Returns a list of possible processor definitions. """
        possibilities = list()

        if not processor_kwargs:
            # if it does not have any arguments, we can use the processor name as a string.
            possibilities.append(processor_name)

        possibilities.append({processor_name:dict(processor_kwargs)})
        possibilities.append(dict(__processor__=processor_name, **processor_kwargs))

        return possibilities


    @defer.inlineCallbacks
    def test_simple_chain(self):
        # source -> uppercase -> reverse -> sink
        pipeline_configuration = {
            'only-pipeline': {
                'chained_consumers': [
                    {'__processor__': 'uppercase'},
                    {'__processor__': 'reverse'},
                ]
            }
        }

        gf = self.get_processor_graph_factory(pipeline_configuration)
        pg = gf.make_processor_graph('only-pipeline')

        self.assertEquals(len(pg.sinks), 1)

        l = []
        sink = ListAppendingProcessor(l)

        last_processor = list(pg.sinks)[0]
        pg.add_producer_and_consumer(last_processor, sink)

        evaluator = processing.TwistedProcessorGraphEvaluator(pg)
        yield evaluator.process('abc')
        self.assertEquals(l, ['CBA'])

    def test_unicode_in_processor_configuration_key_raises(self):
        # source -> uppercase -> reverse -> sink
        pipeline_configuration = {
            'only-pipeline': {
                'chained_consumers': [
                    {'__processor__': 'any', u'æøå':'value'},
                ]
            }
        }

        gf = self.get_processor_graph_factory(pipeline_configuration)

        # non-ascii characters in kwargs keys are not allowed
        self.assertRaises(exceptions.ConfigurationError, gf.make_processor_graph, 'only-pipeline')

    @defer.inlineCallbacks
    def test_failures_are_propagated_to_the_caller(self):
        # source -> raise-exception -> uppercase -> sink
        #                           e> reverse -> sink
        pipeline_configuration = {
            'only-pipeline': {
                'chained_consumers': [
                    {'__processor__': 'raise-exception'},
                ]
            }
        }

        gf = self.get_processor_graph_factory(pipeline_configuration)
        pg = gf.make_processor_graph('only-pipeline')

        evaluator = processing.TwistedProcessorGraphEvaluator(pg)

        try:
            yield evaluator.process('abc')
            self.fail('StubException not raised')
        except StubException:
            pass
        except Exception, e:
            self.fail('Unexpected exception raised: %s'%e)

    @defer.inlineCallbacks
    def test_simple_chain_with_chained_error_consumers(self):
        # source -> raise-exception -> uppercase -> sink
        #                           e> reverse -> sink
        pipeline_configuration = {
            'only-pipeline': {
                'chained_consumers': [
                    {'__processor__': 'raise-exception'},
                    {'__processor__': 'uppercase'},
                ],
                'chained_error_consumers': [
                    {'__processor__': 'reverse'},
                ]
            }
        }

        gf = self.get_processor_graph_factory(pipeline_configuration)
        pg = gf.make_processor_graph('only-pipeline')

        self.assertEquals(len(pg.sinks), 2)

        l = []
        list_appender = ListAppendingProcessor(l)

        for sink in list(pg.sinks):
            pg.add_producer_and_consumer(sink, list_appender)

        evaluator = processing.TwistedProcessorGraphEvaluator(pg)
        yield evaluator.process('abc')
        self.assertEquals(l, ['cba'])

    @defer.inlineCallbacks
    def test_simple_chain_with_error_consumers(self):
        # source -> raise-exception -> uppercase -> sink
        #                           e> reverse -> sink
        pipeline_configuration = {
            'only-pipeline': {
                'chained_consumers': [
                    {'__processor__': 'raise-exception'},
                    {'__processor__': 'uppercase'},
                ],
                'error_consumers': [
                    {'__processor__': 'reverse'},
                ],
            }
        }

        gf = self.get_processor_graph_factory(pipeline_configuration)
        pg = gf.make_processor_graph('only-pipeline')

        self.assertEquals(len(pg.sinks), 2)

        l = []
        list_appender = ListAppendingProcessor(l)

        for sink in list(pg.sinks):
            pg.add_producer_and_consumer(sink, list_appender)

        evaluator = processing.TwistedProcessorGraphEvaluator(pg)
        yield evaluator.process('abc')
        self.assertEquals(l, ['cba'])

    @defer.inlineCallbacks
    def test_simple_chain_with_chained_error_consumers_and_error_consumers(self):
        # source -> raise-exception -> uppercase -> sink
        #                           e> reverse -> uppercase -> sink
        #                           e> reverse -> sink
        pipeline_configuration = {
            'only-pipeline': {
                'chained_consumers': [
                    {'__processor__': 'raise-exception'},
                    {'__processor__': 'uppercase'},
                ],
                'chained_error_consumers':[
                    {'__processor__': 'reverse'},
                    {'__processor__': 'uppercase'},
                ],
                'error_consumers': [
                    {'__processor__': 'reverse'},
                ],
            }
        }

        gf = self.get_processor_graph_factory(pipeline_configuration)
        pg = gf.make_processor_graph('only-pipeline')

        self.assertEquals(len(pg.sinks), 3)

        l = []
        list_appender = ListAppendingProcessor(l)

        for sink in list(pg.sinks):
            pg.add_producer_and_consumer(sink, list_appender)

        evaluator = processing.TwistedProcessorGraphEvaluator(pg)
        yield evaluator.process('abc')
        self.assertEquals(l, ['CBA', 'cba'])

    @defer.inlineCallbacks
    def test_simple_chain_with_error_handler_but_no_exception_raised(self):
        # source -> uppercase -> reverse -> sink
        #                     e> raise-exception -> sink
        pipeline_configuration = {
            'only-pipeline': {
                'chained_consumers': [
                    {'__processor__': 'uppercase'},
                    {'__processor__': 'reverse'},
                ],
                'chained_error_consumers': [
                    {'__processor__': 'raise-exception'},
                ]
            }
        }

        gf = self.get_processor_graph_factory(pipeline_configuration)
        pg = gf.make_processor_graph('only-pipeline')

        self.assertEquals(len(pg.sinks), 2)

        l = []
        sink = ListAppendingProcessor(l)

        last_processor = list(pg.sinks)[0]
        pg.add_producer_and_consumer(last_processor, sink)

        evaluator = processing.TwistedProcessorGraphEvaluator(pg)
        yield evaluator.process('abc')
        self.assertEquals(l, ['CBA'])

    @defer.inlineCallbacks
    def test_simple_tree(self):
        #                 / upper \
        # source - reverse         sink
        #                 \ lower /
        pipeline_configuration = {
            'only-pipeline': {
                    'consumers': [
                        {
                            '__processor__': 'reverse',
                            'consumers': [
                                {'__processor__': 'uppercase'},
                                {'__processor__': 'lowercase'},
                            ]
                        }
                    ]
             }
        }

        gf = self.get_processor_graph_factory(pipeline_configuration)
        pg = gf.make_processor_graph('only-pipeline')

        self.assertEquals(len(pg.sinks), 2)

        l = []
        sink = ListAppendingProcessor(l)

        for processor in list(pg.sinks):
            pg.add_producer_and_consumer(processor, sink)

        evaluator = processing.TwistedProcessorGraphEvaluator(pg)
        yield evaluator.process('aBc')
        self.assertEquals(l, ['CBA', 'cba'])

    @defer.inlineCallbacks
    def test_two_sources_in_a_pipeline(self):
        #       / upper \
        # source         sink
        #       \ lower /
        pipeline_configuration = {
            'only-pipeline': {
                'consumers': [
                    {'__processor__': 'uppercase'},
                    {'__processor__': 'lowercase'},
                ]
            }
        }

        gf = self.get_processor_graph_factory(pipeline_configuration)
        pg = gf.make_processor_graph('only-pipeline')

        self.assertEquals(len(pg.sources), 2)
        self.assertEquals(len(pg.sinks), 2)

        l = []
        sink = ListAppendingProcessor(l)

        for processor in list(pg.sinks):
            pg.add_producer_and_consumer(processor, sink)

        evaluator = processing.TwistedProcessorGraphEvaluator(pg)
        yield evaluator.process('aBc')
        self.assertEquals(l, ['ABC', 'abc'])

    @defer.inlineCallbacks
    def test_simple_dag(self):
        #       /  upper \
        # source          reverse - sink
        #       \  lower /
        pipeline_configuration = {
            'only-pipeline': {
                'consumers': [
                    {
                        '__processor__': 'uppercase',
                        'consumers': [
                            {'__processor__': 'reverse', 'id': 'reuse_me'},
                        ]
                    },
                    {
                        '__processor__': 'lowercase',
                        'consumers': [{'existing': 'reuse_me'}]
                    }
                ]
            }
        }

        gf = self.get_processor_graph_factory(pipeline_configuration)
        pg = gf.make_processor_graph('only-pipeline')

        self.assertEquals(len(pg.sinks), 1)

        l = []
        sink = ListAppendingProcessor(l)

        last_processor = list(pg.sinks)[0]
        pg.add_producer_and_consumer(last_processor, sink)

        evaluator = processing.TwistedProcessorGraphEvaluator(pg)
        yield evaluator.process('aBc')
        self.assertEquals(l, ['CBA', 'cba'])

    def test_nested_pipelines(self):
        pipeline_configuration = {
            'reverse-and-uppercase': {
                'chained_consumers': [
                    {'__processor__': 'reverse'},
                    {'__processor__': 'uppercase'},
                ]
            },
            'reverse-and-lowercase': {
                'chained_consumers': [
                    {'__processor__': 'reverse'},
                    {'__processor__': 'lowercase'},
                ]
            },
            'use-the-others': {
                 'consumers': [
                    {'inline-pipeline': 'reverse-and-uppercase'},
                    {'inline-pipeline': 'reverse-and-lowercase'},
                ]
            },
        }

        expected_pipeline_configuration = {
            'reverse-and-uppercase': {
                'consumers': [
                    {'__processor__': 'reverse',
                     'consumers': [
                            {'__processor__': 'uppercase'},
                        ]
                    }
                ]
            },
            'reverse-and-lowercase': {
                'consumers': [
                    {'__processor__': 'reverse',
                     'consumers': [
                            {'__processor__': 'lowercase'},
                        ]
                    }
                ]
            },
            'use-the-others': {
                 'consumers': [
                    {'__processor__': 'reverse',
                     'consumers': [
                            {'__processor__': 'uppercase'},
                        ]
                    },
                    {'__processor__': 'reverse',
                     'consumers': [
                            {'__processor__': 'lowercase'},
                        ]
                    }
                ]
            },
        }

        self.assertConfigurationProperlyTransformed(pipeline_configuration,
                                                    expected_pipeline_configuration)

    def test_nested_pipelines_with_error_consumers(self):
        pipeline_configuration = {
            'reverse-and-uppercase': {
                'chained_consumers': [
                    {'__processor__': 'reverse'},
                    {'__processor__': 'uppercase'},
                ]
            },
            'reverse-and-lowercase': {
                'chained_consumers': [
                    {'__processor__': 'reverse'},
                    {'__processor__': 'lowercase'},
                ],
                'chained_error_consumers': [{'__processor__': 'reverse'}],
            },
            'use-the-others': {
                 'consumers': [
                    {'inline-pipeline': 'reverse-and-uppercase'},
                    {'inline-pipeline': 'reverse-and-lowercase'},
                ]
            },
        }

        expected_pipeline_configuration = {
            'reverse-and-uppercase': {
                'consumers': [
                    {'__processor__': 'reverse',
                     'consumers': [
                            {'__processor__': 'uppercase'},
                        ]
                    }
                ]
            },
            'reverse-and-lowercase': {
                'consumers': [
                    {'__processor__': 'reverse',
                     'consumers': [
                            {'__processor__': 'lowercase'},
                        ],
                     'error_consumers': [{'__processor__': 'reverse'}],
                    }
                ]
            },
            'use-the-others': {
                 'consumers': [
                    {'__processor__': 'reverse',
                     'consumers': [
                            {'__processor__': 'uppercase'},
                        ]
                    },
                    {'__processor__': 'reverse',
                     'consumers': [
                            {'__processor__': 'lowercase'},
                        ],
                     'error_consumers': [{'__processor__': 'reverse'}],
                    }
                ]
            },
        }

        self.assertConfigurationProperlyTransformed(pipeline_configuration,
                                                    expected_pipeline_configuration)


    def test_consume_nested_pipeline(self):
        pipeline_configuration = {
            'reverse-and-uppercase': {
                'chained_consumers': [
                    {'__processor__': 'reverse'},
                    {'__processor__': 'uppercase'},
                ]
            },
            'consume-the-other': {
                 'chained_consumers': [
                    {'inline-pipeline': 'reverse-and-uppercase'},
                    {'__processor__': 'reverse'},
                ]
            },
        }

        expected_pipeline_configuration = {
            'reverse-and-uppercase': {
                'consumers': [
                    {'__processor__': 'reverse',
                     'consumers': [
                            {'__processor__': 'uppercase'},
                        ]
                    }
                ]
            },
            'consume-the-other': {
                'consumers': [
                    {'__processor__': 'reverse',
                     'consumers': [
                            {'__processor__': 'uppercase',
                             'consumers': [{'__processor__': 'reverse'}]},
                        ]
                    }
                ]
            },
        }

        self.assertConfigurationProperlyTransformed(pipeline_configuration,
                                                    expected_pipeline_configuration)

    def test_consume_nested_pipeline_with_error_handler(self):
        pipeline_configuration = {
            'reverse-and-uppercase': {
                'chained_consumers': [
                    {'__processor__': 'reverse-in-pipeline'},
                    {'__processor__': 'uppercase'},
                ]
            },
            'consume-the-other': {
                 'chained_consumers': [
                    {'inline-pipeline': 'reverse-and-uppercase'},
                    {'__processor__': 'reverse'},
                 ],
                 'chained_error_consumers': [{'inline-pipeline':'reverse-and-uppercase'}]
            },
        }

        expected_pipeline_configuration = {
            'reverse-and-uppercase': {
                'consumers': [
                    {'__processor__': 'reverse-in-pipeline',
                     'consumers': [
                            {'__processor__': 'uppercase'},
                        ]
                    }
                ]
            },
            'consume-the-other': {
                'consumers': [
                    {'__processor__': 'reverse-in-pipeline',
                     'consumers': [
                            {'__processor__': 'uppercase',
                             'consumers': [{'__processor__': 'reverse'}]},
                       ],
                     'error_consumers': [
                         {'__processor__': 'reverse-in-pipeline',
                          'consumers': [
                                 {'__processor__': 'uppercase'},
                             ]
                         }
                     ]
                    }
                ]
            },
        }

        self.assertConfigurationProperlyTransformed(pipeline_configuration,
                                                    expected_pipeline_configuration)


    def test_deeply_nested_pipelines(self):
        pipeline_configuration = {
            'A': {
                'chained_consumers': [
                    {'inline-pipeline': 'B'},
                    {'__processor__': 'P1'},
                ]
            },
            'B': {
                 'chained_consumers': [
                    {'__processor__': 'P2'},
                    {'inline-pipeline': 'C'},
                ]
            },
            'C': {
                'consumers': [{'__processor__': 'P3'}],
            }
        }

        expected_pipeline_configuration = {
            'A': {
                 'consumers': [
                    {'__processor__': 'P2',
                     'consumers': [{'__processor__': 'P3',
                                    'consumers': [
                                    {'__processor__': 'P1'}]}]
                    }]
            },
            'B': {
                 'consumers': [
                    {'__processor__': 'P2',
                     'consumers': [{'__processor__': 'P3'}]}
                ]
            },
            'C': {
                'consumers': [{'__processor__': 'P3'}],
            }
        }

        self.assertConfigurationProperlyTransformed(pipeline_configuration,
                                                    expected_pipeline_configuration)

    def test_deeply_nested_pipelines_with_error_consumers(self):
        # -> = consumer, e> = error_consumer
        # A, B, C, D, E and F are pipelines.
        #
        #  A: B -> P1
        #  B: P2 -> C
        #  C: P3
        #  D: C e> D-handler
        #  E: solo-e
        #     E1 -> D
        #        e> E1-fail-handler
        #  F: solo-f
        #     F1 -> D
        #        e> E

        pipeline_configuration = {
            'A': {
                'chained_consumers': [
                    {'inline-pipeline': 'B'},
                    {'__processor__': 'P1'},
                ]
            },
            'B': {
                 'chained_consumers': [
                    {'__processor__': 'P2'},
                    {'inline-pipeline': 'C'},
                ]
            },
            'C': {
                'consumers': [{'__processor__': 'P3'}],
            },
            'D': {
                'chained_consumers': [
                    {
                        'inline-pipeline':'C',
                    },
                ],
                'chained_error_consumers': [
                    {'__processor__': 'D-handler'}
                ]
            },
            'E': {
                'consumers': [
                    {'__processor__': 'solo-e'},
                    {
                        '__processor__': 'E1',
                        'error_consumers': [{'__processor__': 'E1-fail-handler'}],
                        'consumers': [{'inline-pipeline': 'D'}],
                    }
                ]
            },
            'F': {
                'consumers': [
                    {'__processor__': 'solo-f'},
                    {
                        '__processor__': 'F1',
                        'error_consumers': [{'inline-pipeline': 'E'}],
                        'consumers': [{'inline-pipeline': 'D'}],
                    }
                ]
            }
        }

        expected_pipeline_configuration = {
            'A': {
                 'consumers': [
                    {'__processor__': 'P2',
                     'consumers': [{'__processor__': 'P3',
                                    'consumers': [
                                    {'__processor__': 'P1'}]}]
                    }]
            },
            'B': {
                 'consumers': [
                    {'__processor__': 'P2',
                     'consumers': [{'__processor__': 'P3'}]}
                ]
            },
            'C': {
                'consumers': [{'__processor__': 'P3'}],
            },
            'D': {
                'consumers': [
                    {
                        '__processor__': 'P3',
                        'error_consumers': [
                            {'__processor__':'D-handler'}
                        ]
                    }
                ]
            },
            'E': {
                'consumers': [
                    {'__processor__': 'solo-e'},
                    {
                        '__processor__': 'E1',
                        'error_consumers': [
                            {
                                '__processor__': 'E1-fail-handler',
                            }
                        ],
                        'consumers': [
                            {
                                '__processor__': 'P3',
                                'error_consumers': [
                                    {'__processor__':'D-handler'}
                                ]
                            }
                        ]
                    }
                ]
            },
            'F': {
                'consumers': [
                    {'__processor__': 'solo-f'},
                    {
                        '__processor__': 'F1',
                        'error_consumers': [
                            {'__processor__': 'solo-e'},
                            {
                                '__processor__': 'E1',
                                'error_consumers': [
                                    {
                                        '__processor__': 'E1-fail-handler',
                                    }
                                ],
                                'consumers': [
                                    {
                                        '__processor__': 'P3',
                                        'error_consumers': [
                                            {'__processor__':'D-handler'}
                                        ]
                                    }
                                ]
                            }
                        ],
                        'consumers': [
                            {
                                '__processor__': 'P3',
                                'error_consumers': [
                                    {'__processor__':'D-handler'}
                                ]
                            }
                        ]
                    }
                ]
            }
        }

        self.assertConfigurationProperlyTransformed(pipeline_configuration,
                                                    expected_pipeline_configuration)

    def test_appending_to_chained_error_consumers(self):
        pipeline_configuration = {
            'included-pipeline': {
                'chained_consumers': [
                    {'__processor__': 'do-something'}
                ],
                'chained_error_consumers': [
                    {'__processor__': 'handle-error'},
                ],
            },
            'some-pipeline': {
                'chained_consumers': [
                    {'inline-pipeline': 'included-pipeline'}
                ],
                'chained_error_consumers': [
                    {'__processor__': 'some-error-consumer'},
                ],
            }
        }

        expected_pipeline_configuration = {
            'included-pipeline': {
                'consumers': [
                    {
                        '__processor__': 'do-something',
                        'error_consumers': [{'__processor__': 'handle-error'}],
                    },
                ]
            },
            'some-pipeline': {
                'consumers': [
                    {
                        '__processor__': 'do-something',
                        'error_consumers': [{'__processor__': 'handle-error'}, {'__processor__': 'some-error-consumer'}],
                    }
                ]
            }
        }

        self.assertConfigurationProperlyTransformed(pipeline_configuration, expected_pipeline_configuration)

    def test_error_consumers_chained(self):
        pipeline_configuration = {
            'my-pipeline': {
                'chained_consumers': [
                    {'__processor__': 'do-something'}
                ],
                'chained_error_consumers': [
                    {'__processor__': 'handle-error'},
                    {'__processor__': 'assume-error-is-handled'}
                ],
            },
        }

        expected_pipeline_configuration = {
            'my-pipeline': {
                'consumers': [
                    {
                        '__processor__': 'do-something',
                        'error_consumers': [
                            {
                                '__processor__': 'handle-error',
                                'consumers': [
                                    {'__processor__': 'assume-error-is-handled'}
                                ],
                            }
                        ],
                    },
                ]
            },
        }

        self.assertConfigurationProperlyTransformed(pipeline_configuration, expected_pipeline_configuration)



    def test_consume_nested_pipeline_with_multiple_sinks(self):
        pipeline_configuration = {
            'reverse-and-uppercase': {
                'consumers': [ # Diff to previous test, these are not chained.
                    {'__processor__': 'reverse'},
                    {'__processor__': 'uppercase'},
                ]
            },
            'consume-the-other': {
                 'chained_consumers': [
                    {'inline-pipeline': 'reverse-and-uppercase'},
                    {'__processor__': 'reverse'},
                ]
            },
        }

        expected_pipeline_configuration = {
            'reverse-and-uppercase': {
                'consumers': [
                    {'__processor__': 'reverse'},
                    {'__processor__': 'uppercase'},
                ]
            },
            'consume-the-other': {
                'consumers': [
                    {'__processor__': 'reverse',
                     'consumers': [{'__processor__': 'reverse'}]},
                    {'__processor__': 'uppercase',
                     'consumers': [{'__processor__': 'reverse'}]},
                ]
            },
        }

        self.assertConfigurationProperlyTransformed(pipeline_configuration,
                                                    expected_pipeline_configuration)

    def test_nest_chained_consumers(self):
        pipeline_configuration = {
            'some-pipeline': {
                'chained_consumers': [
                    {'__processor__': 'A'},
                    {'__processor__': 'B'},
                ]
            }
        }

        expected_pipeline_configuration = {
            'some-pipeline': {
                'consumers': [
                    {'__processor__': 'A',
                     'consumers': [
                            {'__processor__': 'B'},
                    ]}
                ]
            }
        }

        self.assertConfigurationProperlyTransformed(pipeline_configuration,
                                                    expected_pipeline_configuration)

    def test_deeply_nest_chained_consumers(self):
        #
        #        / C
        # source - A - D
        #           \ B - E - F
        pipeline_configuration = {
            'some-pipeline': {
                'consumers': [
                    {'__processor__': 'C'},
                ],
                'chained_consumers': [
                    {'__processor__': 'A',
                     'consumers': [{'__processor__':'D'}]
                    },
                    {'__processor__': 'B',
                     'chained_consumers': [
                            {'__processor__': 'E'},
                            {'__processor__': 'F'},
                        ]
                    },
                ]
            }
        }

        expected_pipeline_configuration = {
            'some-pipeline': {
                'consumers': [
                    {'__processor__': 'A',
                     'consumers': [
                            {'__processor__': 'D'},
                            {'__processor__': 'B',
                             'consumers': [
                                    {'__processor__': 'E',
                                     'consumers': [{'__processor__':'F'}]},
                                    ]
                            },
                        ]
                     },
                    {'__processor__': 'C'},
                ]
            }
        }

        self.assertConfigurationProperlyTransformed(pipeline_configuration,
                                                    expected_pipeline_configuration)

    def test_chained_consumers_before_consumers(self):
        pipeline_configuration = {
            'a-pipeline': {
                'chained_consumers': [
                    {'__processor__': 'reverse'},
                ],
                'consumers': [
                    {'__processor__': 'uppercase'},
                ],
                'chained_error_consumers': [
                    {'__processor__': 'reverse'},
                ],
                'error_consumers': [
                    {'__processor__': 'uppercase'},
                ]
            }
        }

        # chained*_consumers should always become the first consumer if both chained*_consumers and consumers
        # are defined:
        expected_pipeline_configuration = {
            'a-pipeline': {
                'consumers': [
                    {
                        '__processor__': 'reverse',
                        'error_consumers': [
                            {'__processor__': 'reverse'},
                            {'__processor__': 'uppercase'},
                        ]
                    },
                    {
                        '__processor__': 'uppercase',
                        'error_consumers': [
                            {'__processor__': 'reverse'},
                            {'__processor__': 'uppercase'},
                        ]
                    },
                ],
            }
        }

        self.assertConfigurationProperlyTransformed(pipeline_configuration, expected_pipeline_configuration)

    def test_pipeline_alias(self):
        pipeline_configuration = {
            'only-pipeline': {
                'chained_consumers': [
                    {'__processor__': 'uppercase'},
                    {'__processor__': 'reverse'},
                ]
            },
            'alias': {
                'consumers': [
                    {'inline-pipeline': 'only-pipeline'},
                ]
            }
        }

        expected_pipeline_configuration = {
            'only-pipeline': {
                'consumers': [
                    {'__processor__': 'uppercase',
                     'consumers': [{'__processor__': 'reverse'}],
                    }
                ]
            },
            'alias': {
                'consumers': [
                    {'__processor__': 'uppercase',
                     'consumers': [{'__processor__': 'reverse'}],
                    }
                ]
            },
        }

        self.assertConfigurationProperlyTransformed(pipeline_configuration,
                                                    expected_pipeline_configuration)

    def test_fails_when_given_invalid_plugin_name(self):
        pipeline_configuration = {
            'only-pipeline': {
                'chained_consumers': [
                    {'__processor__': 'uppercase'},
                    {'__processor__': 'i-do-not-exist'},
                ]
            }
        }

        gf = self.get_processor_graph_factory(pipeline_configuration)
        try:
            gf.make_processor_graph('only-pipeline')
            self.fail('Expected Misconfiguration exception')
        except exceptions.InvalidConfigurationError, e:
            self.assertEquals(e.msg, 'no such plug-in: i-do-not-exist')

    def test_fails_when_configuration_keywords_are_missing(self):

        class MustHaveFoo(StubProcessor):
            interface.classProvides(processing.IProcessor)

            def __init__(self, foo, **kw):
                super(MustHaveFoo, self).__init__(**kw)

        self.plugin_manager.plugin_by_name['must-have-foo'] = MustHaveFoo
        pipeline_configuration = {
            'only-pipeline': {
                'chained_consumers': [
                    {'__processor__': 'must-have-foo', 'notfoo': 'though'}, # but it won't get it
                ]
            }
        }
        gf = self.get_processor_graph_factory(pipeline_configuration)
        try:
            gf.make_processor_graph('only-pipeline')
            self.fail('Expected ConfigurationError')
        except exceptions.ConfigurationError, e:
            expected_e_msg = 'insufficient configuration of processor: must-have-foo'
            expected_detail = "Missing arguments: ['foo']"
            self.assertEquals(e.msg, expected_e_msg)
            self.assertTrue(expected_detail in e.detail)

    def test_fails_when_configuration_keywords_are_unknown(self):

        class MustHaveFoo(StubProcessor):
            interface.classProvides(processing.IProcessor)

            def __init__(self, foo, **kw):
                super(MustHaveFoo, self).__init__(**kw)

        self.plugin_manager.plugin_by_name['must-have-foo'] = MustHaveFoo
        pipeline_configuration = {
            'only-pipeline': {
                'chained_consumers': [
                    {'__processor__': 'must-have-foo', 'foo': 'GOT IT', 'bar': 'nooo'},
                ]
            }
        }
        gf = self.get_processor_graph_factory(pipeline_configuration)
        try:
            gf.make_processor_graph('only-pipeline')
            self.fail('Expected ConfigurationError')
        except exceptions.ConfigurationError, e:
            expected_e_msg = 'invalid keyword "bar" for processor: must-have-foo'
            self.assertEquals(e.msg, expected_e_msg)

    def test_inheriting_pipelines(self):
        pipeline_configuration = {
            'base-pipeline': {
                'chained_consumers': [
                    {'__processor__': 'uppercase',
                     'option': 'to-override',
                     'this-option': 'will be gone'},
                    {'__processor__': 'reverse'},
                ]
            },
            'inheriting-pipeline': {
                'inherits': 'base-pipeline',
                'overrides': [
                    {'__processor__': 'uppercase',
                     'option': 'now overridden'}
                ]
            }
        }

        expected_pipeline_configuration = {
            'base-pipeline': {
                'consumers': [
                    {'__processor__': 'uppercase',
                     'option': 'to-override',
                     'this-option': 'will be gone',
                     'consumers': [{'__processor__': 'reverse'}],
                    }
                ]
            },
            'inheriting-pipeline': {
                'inherits': 'base-pipeline',
                'consumers': [
                    {'__processor__': 'uppercase',
                     'option': 'now overridden',
                     'consumers': [{'__processor__': 'reverse'}],
                    }
                ]
            },
        }

        self.assertConfigurationProperlyTransformed(pipeline_configuration, expected_pipeline_configuration)

    def test_inheriting_fails_with_deep_nesting(self):
        pipeline_configuration = {
            'a': {
                'chained_consumers': [{'__processor__': 'uppercase'}]
            },
            'b': {
                'inherits': 'a',
                'overrides': [{'__processor__': 'uppercase', 'foo': 'bar'}]
            },
            'c': {
                'inherits': 'b',
                'overrides': []
            }
        }

        self.assertRaises(exceptions.ConfigurationError, self.get_processor_graph_factory, pipeline_configuration)

    def test_inheriting_bogus_pipeline(self):
        pipeline_configuration = {
            'a': {
                'chained_consumers': [{'__processor__': 'uppercase'}]
            },
            'b': {
                'inherits': 'not a',
                'overrides': [{'__processor__': 'uppercase', 'foo': 'bar'}]
            },
        }

        self.assertRaises(exceptions.ConfigurationError, self.get_processor_graph_factory, pipeline_configuration)

    def test_not_all_overrides_used(self):
        pipeline_configuration = {
            'a': {
                'chained_consumers': [{'__processor__': 'uppercase'}]
            },
            'b': {
                'inherits': 'a',
                'overrides': [{'__processor__': 'uppercase', 'foo': 'bar'}, {'__processor__': 'reverse', 'bar': 'baz'}],
            },
        }
        self.assertRaises(exceptions.ConfigurationError, self.get_processor_graph_factory, pipeline_configuration)

    def test_inheriting_and_overriding_in_expected_order(self):
        #
        #        / C
        # source - A - D
        #           \ C - E - F
        pipeline_configuration = {
            'some-pipeline': {
                'consumers': [
                    {'__processor__': 'C'},
                ],
                'chained_consumers': [
                    {'__processor__': 'A',
                     'consumers': [{'__processor__':'D'}]
                    },
                    {'__processor__': 'C',
                     'chained_consumers': [
                            {'__processor__': 'E'},
                            {'__processor__': 'F'},
                        ]
                    },
                ]
            },
            'other-pipeline': {
                'inherits': 'some-pipeline',
                'overrides': [
                    {'__processor__': 'C', 'other_option': 'this processor should have E as a consumer'},
                    {'__processor__': 'F', 'foo': 'bar'},
                    {'__processor__': 'C', 'some_option': 'this processor should not have consumers'},
                ]
            }
        }

        expected_pipeline_configuration = {
            'other-pipeline': {
                'consumers': [
                    {'consumers': [{'__processor__': 'D'},
                                   {'consumers': [{'consumers': [{'foo': 'bar', '__processor__': 'F'}],
                                                   '__processor__': 'E'}],
                                    'other_option': 'this processor should have E as a consumer',
                                    '__processor__': 'C'}],
                     '__processor__': 'A'},
                    {'__processor__': 'C', 'some_option': 'this processor should not have consumers'},
                ],
                'inherits': 'some-pipeline'
            },
            'some-pipeline': {
                'consumers': [
                    {'consumers': [
                            {'__processor__': 'D'},
                            {'consumers': [
                                    {'consumers': [{'__processor__': 'F'}],
                                     '__processor__': 'E'}],
                             '__processor__': 'C'}],
                     '__processor__': 'A'},
                    {'__processor__': 'C'},
                ]
            }
        }

        self.assertConfigurationProperlyTransformed(pipeline_configuration, expected_pipeline_configuration)

    def test_inheriting_pipeline_with_new_consumers_fails(self):
        pipeline_configuration = {
            'base-pipeline': {
                'chained_consumers': [
                    {'__processor__': 'uppercase'}
                ]
            },
            'inheriting-pipeline': {
                'inherits': 'base-pipeline',
                'consumers': ['whatever']
            }
        }

        self.assertRaises(exceptions.ConfigurationError, self.get_processor_graph_factory, pipeline_configuration)

    def test_rewriting_pipelines(self):
        expected_pipeline_configuration = dict(
            my_pipeline=dict(
                consumers=[
                    dict(
                        __processor__='uppercase',
                        consumers=[
                            dict(__processor__='lowercase')
                        ]
                    )
                ]
            )
        )

        # test combinations of processor definitions in both chained_consumers and listed notations:

        for uppercase_processor in self.get_processor_definitions('uppercase'):
            for lowercase_processor in self.get_processor_definitions('lowercase'):
                # using list
                pipeline_configuration = dict(my_pipeline=[uppercase_processor, lowercase_processor])
                self.assertConfigurationProperlyTransformed(pipeline_configuration, expected_pipeline_configuration)

                # using chained_consumers
                pipeline_configuration = dict(my_pipeline=dict(chained_consumers=[uppercase_processor, lowercase_processor]))
                self.assertConfigurationProperlyTransformed(pipeline_configuration, expected_pipeline_configuration)

    def test_rewriting_invalid_configurations(self):

        invalid_processor_configurations = [
            (1, 'a number is not a processor definition'),
            (dict(), 'an empty dict'),
            (list(), 'an empty list'),
            (dict(fooar=list()), 'a dict with a non-dict entry'),
            (dict(foo=dict(),bar=dict()), 'a dict with two entries')
        ]

        for invalid_processor_configuration, reason in invalid_processor_configurations:
            invalid_pipeline_configuration = dict(
                my_pipeline=[
                    invalid_processor_configuration
                ]
            )
            # These should not be recognized as pipeline configurations at all..
            with patch.object(processing.log, 'info') as mocked_info:
                self.assertConfigurationProperlyTransformed(invalid_pipeline_configuration, dict(), reason)
                self.assertEquals(mocked_info.call_args_list, [(('No pipeline definitions were found in the configuration.',), {})])

    def test_rewriting_invalid_processor_when_in_pipeline(self):

        invalid_processor_configurations = [
            (1, 'a number is not a processor definition'),
            (dict(), 'an empty dict'),
            (list(), 'an empty list'),
            (dict(fooar=list()), 'a dict with a non-dict entry'),
            (dict(foo=dict(),bar=dict()), 'a dict with two entries')
        ]

        for invalid_processor_configuration, reason in invalid_processor_configurations:
            invalid_pipeline_configuration = dict(
                my_pipeline=dict(chained_consumers=[
                    invalid_processor_configuration
                ])
            )
            # Since these are nested in a pipeline, an attempt will be made to configure the pipeline, but they should
            # fail, pointing at an invalid configuration
            self.assertRaises(exceptions.ConfigurationError, self.get_processor_graph_factory, invalid_pipeline_configuration)

    def test_rewriting_pipelines_with_processor_arguments(self):
        expected_pipeline_configuration = dict(
            my_pipeline=dict(
                consumers=[
                    dict(
                        __processor__='uppercase',
                        uppercaser='UPPERCASER',
                        consumers=[
                            dict(__processor__='lowercase', lowercaser='LOWERCASER')
                        ]
                    )
                ]
            )
        )

        # test combinations of processor definitions in both chained_consumers and listed notations:

        for uppercase_processor in self.get_processor_definitions('uppercase', uppercaser='UPPERCASER'):
            for lowercase_processor in self.get_processor_definitions('lowercase', lowercaser='LOWERCASER'):
                # using list
                pipeline_configuration = dict(my_pipeline=[uppercase_processor, lowercase_processor])
                self.assertConfigurationProperlyTransformed(pipeline_configuration, expected_pipeline_configuration)

                # using chained_consumers
                pipeline_configuration = dict(my_pipeline=dict(chained_consumers=[uppercase_processor, lowercase_processor]))
                self.assertConfigurationProperlyTransformed(pipeline_configuration, expected_pipeline_configuration)

    def test_rewriting_pipelines_in_nested_pipeline(self):
        expected_pipeline_configuration = {
            'namespace1.namespace2.my_pipeline':dict(
                consumers=[
                    dict(
                        __processor__='uppercase',
                        uppercaser='UPPERCASER',
                        consumers=[
                            dict(__processor__='lowercase', lowercaser='LOWERCASER')
                        ]
                    )
                ]
            )
        }

        # test combinations of processor definitions in both chained_consumers and listed notations:

        for uppercase_processor in self.get_processor_definitions('uppercase', uppercaser='UPPERCASER'):
            for lowercase_processor in self.get_processor_definitions('lowercase', lowercaser='LOWERCASER'):
                # using list
                pipeline_configuration = dict(namespace1=dict(namespace2=dict(my_pipeline=[uppercase_processor, lowercase_processor])))
                self.assertConfigurationProperlyTransformed(pipeline_configuration, expected_pipeline_configuration)

                # using chained_consumers
                pipeline_configuration = dict(namespace1=dict(namespace2=dict(my_pipeline=dict(chained_consumers=[uppercase_processor, lowercase_processor]))))
                self.assertConfigurationProperlyTransformed(pipeline_configuration, expected_pipeline_configuration)

    def test_namespaced_imports(self):
        expected_pipeline_configuration = {
            'ns1.ns2.ns3.lower':dict(consumers=[{'__processor__':'lowercaser'}]),
            'ns1.upper':dict(consumers=[{'__processor__':'uppercaser'}]),
            'ns1.ns2.side':dict(consumers=[{'__processor__':'sidecaser'}]),
            'ns1.ns2.my_pipeline':dict(
                consumers=[
                    dict(
                        __processor__='uppercaser',
                        consumers=[
                            dict(__processor__='lowercaser',
                                 consumers=[
                                    dict(__processor__='sidecaser',
                                         consumers=[
                                            dict(__processor__='rootcaser',
                                                consumers=[
                                                    dict(__processor__='rootcaser')
                                                ]
                                            )
                                         ]
                                    )
                                 ]
                            )
                        ]
                    )
                ]
            ),
            'root':dict(consumers=[{'__processor__':'rootcaser'}])
        }

        # create a pipeline that uses different imports:
        pipeline_configuration = dict(
            root=['rootcaser'],
            ns1=dict(
                upper=['uppercaser'],
                ns2=dict(
                    side=['sidecaser'],
                    my_pipeline=[
                        {'inline-pipeline': '..upper'},
                        {'inline-pipeline': '.ns3.lower'},
                        {'inline-pipeline': '.side'}, # sibling import
                        {'inline-pipeline': 'root'}, # absolute import
                        {'inline-pipeline': '...root'},
                    ],
                    ns3=dict(
                        lower=['lowercaser']
                    )
                )
            )
        )

        self.assertConfigurationProperlyTransformed(pipeline_configuration, expected_pipeline_configuration)

    def test_nested_pipelines_with_import(self):
        expected_pipeline_configuration = {
            'namespace1.namespace2.my_pipeline':dict(
                consumers=[
                    dict(
                        __processor__='uppercase',
                        uppercaser='UPPERCASER',
                        consumers=[
                            dict(__processor__='lowercase', lowercaser='LOWERCASER')
                        ]
                    )
                ]
            ),
            'namespace1.namespace2.included_pipeline':dict(
                consumers=[
                    dict(__processor__='lowercase', lowercaser='LOWERCASER')
                ]
            )
        }

        # test combinations of processor definitions in both chained_consumers and listed notations:

        for uppercase_processor in self.get_processor_definitions('uppercase', uppercaser='UPPERCASER'):
            for lowercase_processor in self.get_processor_definitions('lowercase', lowercaser='LOWERCASER'):
                # using list
                pipeline_configuration = dict(namespace1=
                                              dict(namespace2=
                                                   dict(my_pipeline=[uppercase_processor, {'inline-pipeline':'.included_pipeline'}],
                                                        included_pipeline=[lowercase_processor]
                                                   )
                                              )
                                         )
                self.assertConfigurationProperlyTransformed(pipeline_configuration, expected_pipeline_configuration)

                # using chained_consumers:
                pipeline_configuration = dict(namespace1=
                                              dict(namespace2=
                                                   dict(my_pipeline=dict(chained_consumers=[uppercase_processor, {'inline-pipeline':'.included_pipeline'}]),
                                                        included_pipeline=[lowercase_processor]
                                                   )
                                              )
                                         )
                self.assertConfigurationProperlyTransformed(pipeline_configuration, expected_pipeline_configuration)


class FooProvider(StubProcessor):
    provides = ['foo']
    name = 'foo-provider'


class BarProvider(StubProcessor):
    provides = ['bar']
    name = 'bar-provider'


class FooConsumer(StubProcessor):
    depends_on = ['foo']
    name = 'foo-consumer'


class FooConsumer2(StubProcessor):
    depends_on = ['foo']
    name = 'foo-consumer2'


class BarConsumer(StubProcessor):
    depends_on = ['bar']
    name = 'bar-consumer'


class TestMisconfigurationHinting(ProcessorGraphTest):

    def setUp(self):
        super(TestMisconfigurationHinting, self).setUp()
        for plugin_factory in (FooProvider, FooConsumer, FooConsumer2, BarProvider, BarConsumer):
            self.plugin_manager.plugin_by_name[plugin_factory.name] = plugin_factory
            for keyword in plugin_factory.provides:
                self.plugin_manager._providers_by_keyword.setdefault(keyword, []).append(plugin_factory.name)

    def assertEquals(self, a, b, msg=None):
        if not a == b:
            if msg is None:
                msg = ''
            if len(msg) > 0:
                msg += '\n'

            diff = '\n'.join(difflib.context_diff(pprint.pformat(a).split('\n'), pprint.pformat(b).split('\n')))
            diff = diff.split('****')[-1].strip() # Remove the header
            msg = '%sdifferences:\n%s' % (msg, diff)
            raise self.failureException(msg)
        return a

    def test_no_warnings_when_correctly_configured(self):
        pipeline_configuration = {
            'only-pipeline': {
                'chained_consumers': [
                    {'__processor__': 'foo-provider'},
                    {'__processor__': 'bar-provider'},
                    {'__processor__': 'bar-consumer'},
                    {'__processor__': 'foo-consumer'},
                ]
            }
        }

        gf = self.get_processor_graph_factory(pipeline_configuration)
        with warnings.catch_warnings(record=True) as recorded_warnings:
            pg = gf.make_processor_graph('only-pipeline')
        self.assertEqual(pg.configuration_anomalies, [])
        self.assertEqual(len(recorded_warnings), 0)

    def test_warn_that_consumer_precedes_provider(self):
        pipeline_configuration = {
            'only-pipeline': {
                'chained_consumers': [
                    {'__processor__': 'foo-consumer'},
                    {'__processor__': 'foo-provider'},
                    {'__processor__': 'bar-provider'},
                    {'__processor__': 'bar-consumer'},
                ]
            }
        }

        gf = self.get_processor_graph_factory(pipeline_configuration)

        with warnings.catch_warnings(record=True) as recorded_warnings:
            pg = gf.make_processor_graph('only-pipeline')
        expected_anomalies = [dict(processor='foo-consumer', configuration=dict(),
                                   missing_dependencies=set(['foo']),
                                   providers=dict(foo=['foo-provider']))]
        self.assertEquals(pg.configuration_anomalies, expected_anomalies)

        expected_warnings = ['ConfigurationWarning: processor with unsatisfied dependencies: "foo-consumer" in pipeline "only-pipeline"']
        self.assertEquals([repr(w.message) for w in recorded_warnings], expected_warnings)


    def test_multiple_warnings_that_consumers_precede_providers(self):
        pipeline_configuration = {
            'only-pipeline': {
                'chained_consumers': [
                    {'__processor__': 'foo-consumer'},
                    {'__processor__': 'bar-consumer'},
                    {'__processor__': 'foo-provider'},
                    {'__processor__': 'bar-provider'},
                ]
            }
        }

        gf = self.get_processor_graph_factory(pipeline_configuration)

        with warnings.catch_warnings(record=True) as recorded_warnings:
            pg = gf.make_processor_graph('only-pipeline')

        expected_anomalies = [dict(processor='foo-consumer', configuration=dict(),
                                   missing_dependencies=set(['foo'],), providers=dict(foo=['foo-provider'])),
                              dict(processor='bar-consumer', configuration=dict(),
                                   missing_dependencies=set(['bar']), providers=dict(bar=['bar-provider']))]
        self.assertEquals(pg.configuration_anomalies, expected_anomalies)

        expected_warnings = ['ConfigurationWarning: processor with unsatisfied dependencies: "foo-consumer" in pipeline "only-pipeline"',
                             'ConfigurationWarning: processor with unsatisfied dependencies: "bar-consumer" in pipeline "only-pipeline"']
        self.assertEquals([repr(w.message) for w in recorded_warnings], expected_warnings)

    def test_simple_dag(self):
        #       /  foo-consumer2 \
        # source                  foo-consumer - sink
        #       \  foo-provider  /
        pipeline_configuration = {
            'only-pipeline': {
                'consumers': [
                    {
                        '__processor__': 'foo-consumer2',
                        'consumers': [
                            {'__processor__': 'foo-consumer', 'id': 'reuse_me'},
                        ]
                    },
                    {
                        '__processor__': 'foo-provider',
                        'consumers': [{'existing': 'reuse_me'}]
                    }
                ]
            }
        }

        gf = self.get_processor_graph_factory(pipeline_configuration)
        with warnings.catch_warnings(record=True) as recorded_warnings:
            pg = gf.make_processor_graph('only-pipeline')

        expected_anomaly = dict(processor='foo-consumer2', configuration=dict(), missing_dependencies=set(['foo'],),
                                providers=dict(foo=['foo-provider']))

        self.assertEquals(pg.configuration_anomalies, [expected_anomaly])

        expected_warning = 'ConfigurationWarning: processor with unsatisfied dependencies: "foo-consumer2" in pipeline "only-pipeline"'
        self.assertEquals([repr(w.message) for w in recorded_warnings], [expected_warning])


class TestConditional(ProcessorGraphTest):

    def setUp(self):
        ProcessorGraphTest.setUp(self)

        for additional_plugin in (util_processors.LambdaConditional, util_processors.Passthrough):
            self.plugin_manager.plugins.add(additional_plugin)
            self.plugin_manager.plugin_by_name[additional_plugin.name] = additional_plugin

    @defer.inlineCallbacks
    def test_deciding_consumer(self):
        #       /  upper \
        # decide          - reverse - sink
        #       \  lower /
        pipeline_configuration = {
            'only-pipeline': {
                'consumers': [
                    {
                        '__processor__': 'lambda-decider',
                        'lambda': 'input, map=["upper", "lower"]: map.index(input.lower())',
                        'consumers': [
                            {
                                '__processor__': 'uppercase',
                                'consumers': [{'existing': 'exit'}]
                            },
                            {
                                '__processor__': 'lowercase',
                                'consumers': [{'existing': 'exit'}]
                            },
                        ]
                    },
                    {
                        '__processor__': 'reverse',
                        'id': 'exit'
                    },
                ]
            }
        }

        gf = self.get_processor_graph_factory(pipeline_configuration)
        pg = gf.make_processor_graph('only-pipeline')

        l = []
        sink = ListAppendingProcessor(l)

        last_processor = list(pg.sinks)[0]
        pg.add_producer_and_consumer(last_processor, sink)

        evaluator = processing.TwistedProcessorGraphEvaluator(pg)
        evaluator.configure_processors(processing.RuntimeEnvironment())
        yield evaluator.process('uPpEr')
        self.assertEquals(l, ['REPPU', 'rEpPu'])
        del l[:]
        yield evaluator.process('lOwEr')
        self.assertEquals(l, ['rewol', 'rEwOl'])


class TwistedEvaluatorTest(ProcessorGraphTest):

    def test_processor_indices(self):
        pg = processing.ProcessorGraph()
        builder = pg.get_builder()
        passthrough = util_processors.Passthrough()
        incrementing = IncrementingProcessor()
        waiter = util_processors.Waiter(0)

        builder.add_processor(passthrough).add_processor(incrementing)
        builder.add_processor(waiter)

        evaluator = processing.TwistedProcessorGraphEvaluator(pg, name='test_pipeline')

        self.assertEquals([evaluator[i] for i in range(3)], [passthrough, incrementing, waiter])
        self.assertEquals([evaluator[-i] for i in range(1, 4)], [waiter, incrementing, passthrough])

    @defer.inlineCallbacks
    def test_just_one_processor(self):
        l = []
        only_processor = ListAppendingProcessor(l)
        pipeline = processing.ProcessorGraph()
        pipeline.get_builder().add_processor(only_processor)
        evaluator = processing.TwistedProcessorGraphEvaluator(pipeline)
        yield evaluator.process('foo')
        self.assertEquals(l, ['foo'])

    @defer.inlineCallbacks
    def test_simple_pipeline(self):
        uppercaser = UppercasingProcessor()
        reverser = ReversingProcessor()
        l = []
        sink = ListAppendingProcessor(l)

        pipeline = processing.ProcessorGraph()

        builder = pipeline.get_builder()
        builder.add_processor(uppercaser).add_processor(reverser).add_processor(sink)
        evaluator = processing.TwistedProcessorGraphEvaluator(pipeline)
        yield evaluator.process('foo')
        yield evaluator.process('bar')
        self.assertEquals(l, ['OOF', 'RAB'])

    @defer.inlineCallbacks
    def test_simple_pipeline_with_callbacking_processor(self):
        uppercaser = UppercasingProcessor()
        reverser = ReversingProcessor()
        later = CallbackingLaterProcessor()
        later2 = CallbackingLaterProcessor()
        l = []
        sink = ListAppendingProcessor(l)

        pipeline = processing.ProcessorGraph()

        builder = pipeline.get_builder()
        builder.add_processor(uppercaser).add_processor(later).add_processor(reverser).add_processor(later2).add_processor(sink)
        evaluator = processing.TwistedProcessorGraphEvaluator(pipeline)
        yield evaluator.process('foo')
        yield evaluator.process('bar')
        self.assertEquals(l, ['OOF', 'RAB'])

    @defer.inlineCallbacks
    def test_broadcast(self):
        collected_by_a = []
        a = ListAppendingProcessor(collected_by_a)
        collected_by_b = []
        b = ListAppendingProcessor(collected_by_b)

        pg = processing.ProcessorGraph()
        builder = pg.get_builder()
        builder.add_processor(UppercasingProcessor()).add_processor(a)
        builder.add_processor(ReversingProcessor()).add_processor(b)

        evaluator = processing.TwistedProcessorGraphEvaluator(pg)
        yield evaluator.process('abc')

        self.assertEquals(collected_by_a, ['ABC'])
        self.assertEquals(collected_by_b, ['cba'])

    @defer.inlineCallbacks
    def test_consume_multiple(self):
        l = []
        sink = ListAppendingProcessor(l)

        pg = processing.ProcessorGraph()
        builder = pg.get_builder()
        builder.add_processors(UppercasingProcessor(), ReversingProcessor()).add_processor(sink)

        evaluator = processing.TwistedProcessorGraphEvaluator(pg)
        yield evaluator.process('abc')

        self.assertEquals(l, ['ABC', 'cba'])

    @defer.inlineCallbacks
    def test_consume_multiple_ordering_matters(self):
        l = []
        sink = ListAppendingProcessor(l)

        pg = processing.ProcessorGraph()
        builder = pg.get_builder()
        builder.add_processors(ReversingProcessor(), UppercasingProcessor()).add_processor(sink)

        evaluator = processing.TwistedProcessorGraphEvaluator(pg)
        yield evaluator.process('abc')

        self.assertEquals(l, ['cba', 'ABC'])

    @defer.inlineCallbacks
    def test_broadcast_and_consume_multiple(self):
        l = []
        sink = ListAppendingProcessor(l)
        pg = processing.ProcessorGraph()
        builder = pg.get_builder()
        reversing = builder.add_processor(ReversingProcessor())
        upper_and_lower = reversing.add_processors(UppercasingProcessor(),
                                                   LowercasingProcessor())
        upper_and_lower.add_processor(sink)

        evaluator = processing.TwistedProcessorGraphEvaluator(pg)
        yield evaluator.process('aBc')

        self.assertEquals(l, ['CBA', 'cba'])

    @defer.inlineCallbacks
    def test_broadcast_and_consume_multiple_and_ordering_matters(self):
        l = []
        sink = ListAppendingProcessor(l)
        pg = processing.ProcessorGraph()
        builder = pg.get_builder()
        reversing = builder.add_processor(ReversingProcessor())
        upper_and_lower = reversing.add_processors(LowercasingProcessor(),
                                                   UppercasingProcessor())
        upper_and_lower.add_processor(sink)

        evaluator = processing.TwistedProcessorGraphEvaluator(pg)
        yield evaluator.process('aBc')

        self.assertEquals(l, ['cba', 'CBA'])

    @defer.inlineCallbacks
    def test_graph_with_loop(self):
        passthrough = util_processors.Passthrough()
        incrementer = util_processors.LambdaProcessor(input_path='n', **{'lambda': 'n: n+1'})
        decider = util_processors.LambdaConditional(**{'lambda':"baton: -1 if baton['n'] > 9 else 0"})
        l = []
        sink = ListAppendingProcessor(l)

        # passthrough -> incrementer -> decider -> sink
        #                    ^------------/
        pg = processing.ProcessorGraph()
        builder = pg.get_builder()
        # The passthrough is necessary because "incrementer" will be
        # removed from the "sources"-list in the processor-graph,
        # because it has parents.
        builder = builder.add_processor(passthrough).add_processor(incrementer).add_processor(decider)
        builder.add_processor(incrementer)
        builder.add_processor(sink)

        runtime_environment = processing.RuntimeEnvironment()
        runtime_environment.configure()
        evaluator = processing.TwistedProcessorGraphEvaluator(pg)
        evaluator.configure_processors(runtime_environment)

        baton = dict(n=0)
        yield evaluator.process(baton)
        self.assertEquals(baton, dict(n=10))

    @defer.inlineCallbacks
    def test_profiling(self):
        """ The evaluator should track the time spent processing, as
        well as the time individual processors spend.
        """
        pg = processing.ProcessorGraph()
        builder = pg.get_builder()
        delay = 0.001

        waiter = util_processors.Waiter(delay)
        waiter2 = util_processors.Waiter(2 * delay)
        builder.add_processor(waiter).add_processor(waiter2)

        evaluator = processing.TwistedProcessorGraphEvaluator(pg)
        yield evaluator.process(dict())

        # wait for delay -> wait for two times delay
        self.assertTrue(delay <= waiter.time_spent)
        self.assertTrue(2*delay <= waiter2.time_spent)
        self.assertEquals(evaluator.time_spent, waiter.time_spent + waiter2.time_spent)

    test_profiling.timeout=1

    def assertTraceEquals(self, actual, expected):
        # set some defaults in order to make the calls to this function easier to read
        for step in expected:
            step.setdefault('is_error_consumer', False)
            step.setdefault('failure', None)

        # we don't want to have to compare the actual traceback messages and timings
        for step in actual:
            step.pop('time_spent', None)
            if step['failure']:
                del step['failure']['traceback']

        self.assertEquals(actual, expected)

    @defer.inlineCallbacks
    def test_simple_tracing(self):
        pg = processing.ProcessorGraph()
        builder = pg.get_builder()
        passthrough = util_processors.Passthrough()
        incrementing = IncrementingProcessor()

        builder.add_processor(passthrough).add_processor(incrementing)

        evaluator = processing.TwistedProcessorGraphEvaluator(pg, name='test_pipeline')
        results, trace = yield evaluator.traced_process(dict(n=0))

        self.assertEquals(results, [dict(n=1)])
        self.assertTraceEquals(trace, [
            dict(source=self, destination=passthrough, baton=dict(n=0)),
            dict(source=passthrough, destination=incrementing, baton=dict(n=0)),
            dict(source=incrementing, destination=None, baton=dict(n=1))
        ])

    @defer.inlineCallbacks
    def test_tracing_across_pipelines(self):
        runtime_environment = processing.RuntimeEnvironment()
        runtime_environment.configure()

        nested_pg = processing.ProcessorGraph()
        nested_incrementing = IncrementingProcessor()
        nested_pg.get_builder().add_processor(nested_incrementing)
        nested_evaluator = processing.TwistedProcessorGraphEvaluator(nested_pg, name='test_pipeline2')
        
        pg = processing.ProcessorGraph()
        builder = pg.get_builder()

        passthrough = util_processors.Passthrough()
        incrementing = IncrementingProcessor()
        run_pipeline = pipeline_processors.PipelineRunner(pipeline='test_pipeline2')

        builder.add_processor(passthrough).add_processor(incrementing).add_processor(run_pipeline)
        evaluator = processing.TwistedProcessorGraphEvaluator(pg, name='test_pipeline')

        evaluator.configure_processors(runtime_environment)
        run_pipeline.pipeline_dependency.is_ready = True
        run_pipeline.pipeline_dependency.on_resource_ready(nested_evaluator)
        
        results, trace = yield evaluator.traced_process(dict(n=0))

        self.assertEquals(results, [dict(n=2)])
        self.assertTraceEquals(trace, [
            dict(source=self, destination=passthrough, baton=dict(n=0)),
            dict(source=passthrough, destination=incrementing, baton=dict(n=0)),
            dict(source=incrementing, destination=run_pipeline, baton=dict(n=1)),
            # run_pipeline -> to the nested pipeline
            dict(source=run_pipeline, destination=nested_incrementing, baton=dict(n=1)),
            dict(source=nested_incrementing, destination=None, baton=dict(n=2)),
            dict(source=run_pipeline, destination=None, baton=dict(n=2)),
        ])

    @defer.inlineCallbacks
    def test_tracing_across_pipelines_with_delayed_failures(self):
        """ If a target pipeline raises an exception asynchronously, much of the stack is
        lost, but tracing should still work. """
        runtime_environment = processing.RuntimeEnvironment()
        runtime_environment.configure()

        nested_pg = processing.ProcessorGraph()
        nested_raiser = DelayedErrbackProcessor()
        nested_pg.get_builder().add_processor(nested_raiser)
        nested_evaluator = processing.TwistedProcessorGraphEvaluator(nested_pg, name='test_pipeline2')

        pg = processing.ProcessorGraph()
        builder = pg.get_builder()

        passthrough = util_processors.Passthrough()
        incrementing = IncrementingProcessor()
        for_each = pipeline_processors.ForEach(pipeline='test_pipeline2', input_path='for_each_input', output_path=None)

        builder.add_processor(passthrough).add_processor(incrementing).add_processor(for_each)
        evaluator = processing.TwistedProcessorGraphEvaluator(pg, name='test_pipeline')

        evaluator.configure_processors(runtime_environment)
        for_each.pipeline_dependency.is_ready = True
        for_each.pipeline_dependency.on_resource_ready(nested_evaluator)

        results, trace = yield evaluator.traced_process(dict(n=0, for_each_input=[1,2]))

        self.assertEquals(results, [dict(n=1, for_each_input=[1,2])])
        self.assertTraceEquals(trace, [
            dict(source=self, destination=passthrough, baton=dict(n=0, for_each_input=[1,2])),
            dict(source=passthrough, destination=incrementing, baton=dict(n=0, for_each_input=[1,2])),
            dict(source=incrementing, destination=for_each, baton=dict(n=1, for_each_input=[1,2])),

            # for_each -> to the nested pipeline
            dict(source=for_each, destination=nested_raiser, baton=1),
            dict(source=nested_raiser, destination=for_each, baton=1, failure=dict(type=StubException, args=())),

            # the for_each processor ignores the error and processes the second baton:
            dict(source=for_each, destination=nested_raiser, baton=2),
            dict(source=nested_raiser, destination=for_each, baton=2, failure=dict(type=StubException, args=())),

            dict(source=for_each, destination=None, baton=dict(n=1, for_each_input=[1,2])),
        ])

    @defer.inlineCallbacks
    def test_tracing_across_pipelines_via_proxy_object(self):
        """ If any non-pipeline proxy object invokes a pipeline, it should show up
        in the trace results.
        """
        runtime_environment = processing.RuntimeEnvironment()
        runtime_environment.configure()

        nested_pg = processing.ProcessorGraph()
        nested_incrementing = IncrementingProcessor()
        nested_pg.get_builder().add_processor(nested_incrementing)
        nested_evaluator = processing.TwistedProcessorGraphEvaluator(nested_pg, name='test_pipeline2')

        class EvaluatorProxy(object):
            def __init__(self, evaluator):
                self.evaluator = evaluator

            def __call__(self, baton):
                return self.evaluator(baton)

        nested_evaluator_proxy = EvaluatorProxy(nested_evaluator)

        pg = processing.ProcessorGraph()
        builder = pg.get_builder()

        passthrough = util_processors.Passthrough()
        incrementing = IncrementingProcessor()
        run_pipeline = pipeline_processors.PipelineRunner(pipeline='test_pipeline2')

        builder.add_processor(passthrough).add_processor(incrementing).add_processor(run_pipeline)
        evaluator = processing.TwistedProcessorGraphEvaluator(pg, name='test_pipeline')

        evaluator.configure_processors(runtime_environment)
        run_pipeline.pipeline_dependency.is_ready = True
        run_pipeline.pipeline_dependency.on_resource_ready(nested_evaluator_proxy)

        results, trace = yield evaluator.traced_process(dict(n=0))

        self.assertEquals(results, [dict(n=2)])
        self.assertTraceEquals(trace, [
            dict(source=self, destination=passthrough, baton=dict(n=0)),
            dict(source=passthrough, destination=incrementing, baton=dict(n=0)),
            dict(source=incrementing, destination=run_pipeline, baton=dict(n=1)),
            dict(source=nested_evaluator_proxy, destination=nested_incrementing, baton=dict(n=1)),
            dict(source=nested_incrementing, destination=None, baton=dict(n=2)),
            dict(source=run_pipeline, destination=None, baton=dict(n=2)),
        ])

    @defer.inlineCallbacks
    def test_tracing_is_unaffected_by_simultaneous_processing(self):
        """ If something else causes a baton to be processed in a pipeline, the tracing should
        not be affected by it. """
        pg = processing.ProcessorGraph()
        builder = pg.get_builder()

        passthrough = util_processors.Passthrough()
        waiter = util_processors.Waiter(delay=0.1)
        incrementing = IncrementingProcessor()

        builder.add_processor(passthrough).add_processor(waiter).add_processor(incrementing)

        evaluator = processing.TwistedProcessorGraphEvaluator(pg, name='test_pipeline')

        traced_process_deferred = evaluator.traced_process(dict(n=0))
        # this will cause _process to be called with another stack
        yield evaluator.process(dict(n=0))

        results, trace = yield traced_process_deferred

        self.assertEquals(results, [dict(n=1)])
        self.assertTraceEquals(trace, [
            dict(source=self, destination=passthrough, baton=dict(n=0)),
            dict(source=passthrough, destination=waiter, baton=dict(n=0)),
            dict(source=waiter, destination=incrementing, baton=dict(n=0)),
            dict(source=incrementing, destination=None, baton=dict(n=1))
        ])

    @defer.inlineCallbacks
    def test_tracing_across_asynchronous_pipelines(self):
        """ Pipelines that process batons asynchronously should still be possible to trace. """
        runtime_environment = processing.RuntimeEnvironment()
        runtime_environment.configure()

        def create_async_pipeline(pipeline_name, last=False):
            pg = processing.ProcessorGraph()
            waiter = util_processors.Waiter(0)
            incrementer = IncrementingProcessor()
            builder = pg.get_builder().add_processor(waiter).add_processor(incrementer)
            if not last:
                run_pipeline = pipeline_processors.PipelineRunner('test_pipeline')
                builder.add_processor(run_pipeline)

            evaluator = processing.TwistedProcessorGraphEvaluator(pg, name=pipeline_name)
            evaluator.configure_processors(runtime_environment)

            return evaluator

        async_pipeline = create_async_pipeline('test_pipeline')
        async_pipeline2 = create_async_pipeline('test_pipeline2')
        async_pipeline3 = create_async_pipeline('test_pipeline3', last=True)

        # hook up the run-pipeline processors (the last processor in the pipelines) to
        # the next pipeline:
        async_pipeline[-1].pipeline_dependency.is_ready = True
        async_pipeline[-1].pipeline_dependency.on_resource_ready(async_pipeline2)

        async_pipeline2[-1].pipeline_dependency.is_ready = True
        async_pipeline2[-1].pipeline_dependency.on_resource_ready(async_pipeline3)


        results, trace = yield async_pipeline.traced_process(dict(n=0))

        self.assertEquals(results, [dict(n=3)])
        self.assertTraceEquals(trace, [
            # source -> pipeline 1
            dict(source=self, destination=async_pipeline[0], baton=dict(n=0)),

            # pipeline 1
            dict(source=async_pipeline[0], destination=async_pipeline[1], baton=dict(n=0)),
            dict(source=async_pipeline[1], destination=async_pipeline[2], baton=dict(n=1)),

            # pipeline 1 -> pipeline 2
            dict(source=async_pipeline[2], destination=async_pipeline2[0], baton=dict(n=1)),

            # pipeline 2
            dict(source=async_pipeline2[0], destination=async_pipeline2[1], baton=dict(n=1)),
            dict(source=async_pipeline2[1], destination=async_pipeline2[2], baton=dict(n=2)),

            # pipeline 2 -> pipeline 3
            dict(source=async_pipeline2[2], destination=async_pipeline3[0], baton=dict(n=2)),

            # pipeline 3
            dict(source=async_pipeline3[0], destination=async_pipeline3[1], baton=dict(n=2)),

            # pipeline 3 finishes
            dict(source=async_pipeline3[1], destination=None, baton=dict(n=3)),
            # which cascades to the pipeline runners finishing
            dict(source=async_pipeline2[-1], destination=None, baton=dict(n=3)),
            dict(source=async_pipeline[-1], destination=None, baton=dict(n=3))

        ])

    @defer.inlineCallbacks
    def test_tracing_unhandled_exception(self):
        """ Unhandled exceptions during tracing should end up in the trace results. """
        runtime_environment = processing.RuntimeEnvironment()
        runtime_environment.configure()

        nested_pg = processing.ProcessorGraph()
        nested_raiser = ExceptionRaisingProcessor()
        nested_incrementing = util_processors.Passthrough()
        nested_pg.get_builder().add_processor(nested_raiser).add_processor(nested_incrementing)
        nested_evaluator = processing.TwistedProcessorGraphEvaluator(nested_pg, name='test_pipeline2')

        pg = processing.ProcessorGraph()
        builder = pg.get_builder()

        run_pipeline = pipeline_processors.PipelineRunner(pipeline='test_pipeline2')

        builder.add_processor(run_pipeline)
        evaluator = processing.TwistedProcessorGraphEvaluator(pg, name='test_pipeline')

        evaluator.configure_processors(runtime_environment)
        run_pipeline.pipeline_dependency.is_ready = True
        run_pipeline.pipeline_dependency.on_resource_ready(nested_evaluator)

        results, trace = yield evaluator.traced_process(dict(n=0))

        self.assertIsInstance(results, failure.Failure)
        self.assertEquals(results.type, StubException)
        
        self.assertTraceEquals(trace, [
            dict(source=self, destination=run_pipeline, baton=dict(n=0)),
            dict(source=run_pipeline, destination=nested_raiser, baton=dict(n=0)),
            dict(source=nested_raiser, destination=run_pipeline, baton=dict(n=0), failure=dict(type=StubException, args=())),
            dict(source=run_pipeline, destination=self, baton=dict(n=0), failure=dict(type=StubException, args=())),
        ])

    @defer.inlineCallbacks
    def test_tracing_handled_exception(self):
        """ Handled exceptions should not continuously show up in the trace results. """
        runtime_environment = processing.RuntimeEnvironment()
        runtime_environment.configure()

        nested_pg = processing.ProcessorGraph()
        nested_raiser = ExceptionRaisingProcessor()
        nested_handler = util_processors.Passthrough()
        nested_incrementing = IncrementingProcessor()
        nested_pg.get_builder().add_processor(nested_raiser).add_processor(nested_handler, is_error_consumer=True).add_processor(nested_incrementing)
        nested_evaluator = processing.TwistedProcessorGraphEvaluator(nested_pg, name='test_pipeline2')

        pg = processing.ProcessorGraph()
        builder = pg.get_builder()

        run_pipeline = pipeline_processors.PipelineRunner(pipeline='test_pipeline2')

        builder.add_processor(run_pipeline)
        evaluator = processing.TwistedProcessorGraphEvaluator(pg, name='test_pipeline')

        evaluator.configure_processors(runtime_environment)
        run_pipeline.pipeline_dependency.is_ready = True
        run_pipeline.pipeline_dependency.on_resource_ready(nested_evaluator)

        results, trace = yield evaluator.traced_process(dict(n=0))

        self.assertEquals(results, [dict(n=1)])

        self.assertTraceEquals(trace, [
            dict(source=self, destination=run_pipeline, baton=dict(n=0)),
            dict(source=run_pipeline, destination=nested_raiser, baton=dict(n=0)),
            dict(source=nested_raiser, destination=nested_handler, baton=dict(n=0), failure=dict(type=StubException, args=()), is_error_consumer=True),
            dict(source=nested_handler, destination=nested_incrementing, baton=dict(n=0)),
            dict(source=nested_incrementing, destination=None, baton=dict(n=1)),
            dict(source=run_pipeline, destination=None, baton=dict(n=1)),
        ])

    @defer.inlineCallbacks
    def test_trace_consumer_raises(self):
        pg = processing.ProcessorGraph()
        builder = pg.get_builder()
        passthrough = util_processors.Passthrough()
        raiser = ExceptionRaisingProcessor()

        builder.add_processor(passthrough).add_processor(raiser)

        evaluator = processing.TwistedProcessorGraphEvaluator(pg, name='test_pipeline')
        results, trace = yield evaluator.traced_process(dict(n=0))

        self.assertIsInstance(results, failure.Failure)
        self.assertEquals(results.type, StubException)
        self.assertTraceEquals(trace, [
            dict(source=self, destination=passthrough, baton=dict(n=0)),
            dict(source=passthrough, destination=raiser, baton=dict(n=0)),
            dict(source=raiser, destination=passthrough, baton=dict(n=0), failure=dict(type=StubException, args=())),
            dict(source=passthrough, destination=self, baton=dict(n=0), failure=dict(type=StubException, args=()))
        ])

    @defer.inlineCallbacks
    def test_trace_error_consumer_raises(self):
        pg = processing.ProcessorGraph()
        builder = pg.get_builder()
        passthrough = util_processors.Passthrough()
        raiser = ExceptionRaisingProcessor()
        error_raiser = ExceptionRaisingProcessor()

        builder.add_processor(passthrough).add_processor(raiser).add_processor(error_raiser, is_error_consumer=True)

        evaluator = processing.TwistedProcessorGraphEvaluator(pg, name='test_pipeline')
        results, trace = yield evaluator.traced_process(dict(n=0))

        self.assertIsInstance(results, failure.Failure)
        self.assertEquals(results.type, StubException)
        self.assertTraceEquals(trace, [
            dict(source=self, destination=passthrough, baton=dict(n=0)),
            dict(source=passthrough, destination=raiser, baton=dict(n=0)),
            dict(source=raiser, destination=error_raiser, baton=dict(n=0), failure=dict(type=StubException, args=()), is_error_consumer=True),
            dict(source=error_raiser, destination=raiser, baton=dict(n=0), failure=dict(type=StubException, args=())),
            dict(source=raiser, destination=passthrough, baton=dict(n=0), failure=dict(type=StubException, args=())),
            dict(source=passthrough, destination=self, baton=dict(n=0), failure=dict(type=StubException, args=()))
        ])

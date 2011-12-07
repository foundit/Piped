# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.

""" Handle processor graphs --- i.e. wiring processors together in
dataflow graphs and evaluating them.

"""
import collections
import inspect
import pprint
import time
import warnings
import sys
import uuid
import json
from copy import deepcopy

from twisted.application import service
from twisted.internet import defer
from twisted.plugin import IPlugin
from twisted.python import failure
from zope import interface

from piped import exceptions, graph, log, util, conf, resource, dependencies, processors as piped_processors, plugin, plugins


class IProcessor(IPlugin):
    """ Defines the interface for all processors that are used in the pipelines.

    The processor will be be given the configuration options as keyword arguments
    to its ``__init__`` function.
    """

    name = interface.Attribute("""Short name of the processor, by which it will be referenced in
        configuration files.""")

    id = interface.Attribute('Unique ID of the processor, that identifies the instance '
        'of the processor in a processor graph. Auto-assigned if not configured manually.')

    processor_configuration = interface.Attribute('Assigned by the graph factory, it is the '
        'configuration used to initialize this processor instance.')

    evaluator = interface.Attribute('Assigned by the evaluator before the processor is configured.')

    consumers = interface.Attribute('Consumers of this processor. This is a list that the '
        'processor graph builder appends consumers during configuration')

    error_consumers = interface.Attribute('Error consumers of this processor. This is a list that the '
        'processor graph builder appends consumers during configuration')

    provides = interface.Attribute('List of keywords the processors of this class provides.')
    depends_on = interface.Attribute('List of keywords the processors of this class depends on.')

    instance_provides = interface.Attribute('List of keywords this specific instance of the '
        'processor provides.')

    instance_depends_on = interface.Attribute('List of keywords this specific instance of the '
        'processor depends on.')

    state = interface.Attribute('Property that returns a dictionary representing the current state. '
        'Used for debugging. Note that the returned state must contain copies of any state that '
        'could change.')

    def configure(self, runtime_environment):
        """ Configures the processor with the runtime environment.

        This is where the processors usually will request dependencies and prepare itself
        for processing.
        """

    def process(self, baton):
        """ Processes a baton.

        This function is called by the evaluator when it is time for a processor to process a baton. The
        return value of this function is used as the input baton for consumers of this processor.
        """

    def get_consumers(self, baton):
        """ Return a list of consumers of this processor.

        This function is called after :func:`process` with the returned baton, and the consumers
        returned by this function are used by the evaluator when evaluating the remaining processing
        graph.

        The processors may use this function to affect the sequence or list of consumers.

        :retval: `list` of :class:`IProcessor`\s
        """

    def get_error_consumers(self, baton):
        """ Return a list of error consumers of this processor.

        See :func:`get_consumers`
        """


class ProcessorGraphBuilder(object):
    """ Utility to wire :class:`~piped.pipelines.ProcessorGraph`.

    E.g. to build a processor graph that makes processor *A* output to
    *B* and *C*, and then have *D* consume both *B* and *C*, do ::

       >>> pg = ProcessorGraph()
       >>> pg.get_builder().add_processor(A).add_processors(B, C).add_processor(D)

    :note: Do not instantiate `ProcessorGraphBuilder` directly ---
        call :meth:`~piped.pipelines.ProcessorGraph.get_builder()`.

    """

    def __init__(self, processor_graph, *producers):
        """ Create a `ProcessorGraphBuilder` for *processor_graph*'s
        *producers'. """
        self.processor_graph = processor_graph
        self.producers = producers

    def add_processor(self, consumer, is_error_consumer=False):
        """ Add *consumer* as a consumer to the wrapped producers, and
        return a builder where *consumer* is the producer to add
        consumers to. """
        for producer in self.producers:
            self.processor_graph.add_producer_and_consumer(producer, consumer, is_error_consumer)
        return ProcessorGraphBuilder(self.processor_graph, consumer)

    def add_processors(self, *consumers):
        """ Same as :meth:`add_processor`, but attaches a list of
        consumers to each of the current producers. """
        for producer in self.producers:
            for consumer in consumers:
                self.processor_graph.add_producer_and_consumer(producer, consumer)
        return ProcessorGraphBuilder(self.processor_graph, *consumers)


class ProcessorGraph(object):
    """ Holds a graph of processors and their producer-consumer
    relationships. Used in conjunction with
    :class:`ProcessorGraphEvaluator`. """

    def __init__(self):
        self.consumers = graph.DirectedGraphWithOrderedEdges()
        # We want to preserve ordering, so we use lists instead of
        # sets. The lists will be small, so looking up won't take much
        # time anyway.
        self.sources = []
        self.sinks = []
        self.entry_point = object() # sentinel to make the graph builder simpler.
        self.configuration_anomalies = []
        self.suppress_warnings = False
        self._processor_ids = collections.defaultdict(lambda: 0)
        self.is_configured = False

    def get_builder(self):
        """ Returns a builder object, whose consumers will be sources
        in the processor graph. """
        return ProcessorGraphBuilder(self, self.entry_point)

    def add_producer_and_consumer(self, producer, consumer, is_error_consumer=False):
        """ Add a producer-consumer-relationship between *producer* and *consumer*.

        :param producer: The processor whose output will be consumed.
        :param consumer: The processor which will be a consumer of producer.
        """
        if not isinstance(consumer, basestring):
            self._ensure_processor_has_id(consumer)
        if producer is self.entry_point:
            # It's the sentinel we made above, so it'll be a source.
            self.consumers.add_node(consumer)
            if not consumer in self.sources:
                self.sources.append(consumer)
        else:
            if not isinstance(producer, basestring):
                self._ensure_processor_has_id(producer)
            self.consumers.add_edge(producer, consumer, dict(is_error_consumer=is_error_consumer))

        if producer in self.sinks and not is_error_consumer:
            # Having an error consumer does not disqualify a processor from being a sink.
            self.sinks.remove(producer)
        if not consumer in self.sinks:
            self.sinks.append(consumer)

    def _ensure_processor_has_id(self, processor):
        """ Ensure that *processor* has an ID that uniquely identifies
        that instance in this graph.

        When there are multiple instances of the same processor in a
        graph, its name alone does not uniquely identify it. """
        if processor.id is not None:
            return

        processor_name = processor.name
        self._processor_ids[processor_name] += 1
        id_ = u"%s-%i" % (processor_name, self._processor_ids[processor_name])
        processor.id = id_

    def __iter__(self):
        """ Iterates over all the processors in the pipeline. """
        return iter(self.consumers)

    def __getitem__(self, item):
        return self.consumers.nodes()[item]

    def get_all_processors_preceding(self, processor):
        # TODO: Refactor f.q.querygraph to be based on a general
        # graph-structure we then reuse here..
        if processor not in self.consumers:
            return

        # There can be loops in the graph. Don't get caught in those.
        already_visited = set()

        queue = collections.deque(self.consumers.predecessors(processor))
        while queue:
            item = queue.popleft()
            yield item
            for predecessor in self.consumers.predecessors(item):
                if predecessor not in already_visited:
                    queue.append(predecessor)
                    already_visited.add(predecessor)

    def configure_processors(self, runtime_environment):
        """ Configure all processors with the provided kwargs. """
        if self.is_configured:
            return
        for processor in self:
            processor.configure(runtime_environment)
        self.is_configured = True

    def get_dot(self, draw_dependency_edges=False):
        nodes = []
        edges = []

        processors = list(self)
        evaluator_time = processors[0].evaluator.time_spent
        mean_time = sum([processor.time_spent for processor in processors]) / len(processors)

        for processor in self:
            # Color the border of the node according to how much time has been spent processing in that node.
            color = self._get_color_for_time(processor.time_spent, mean_time)

            if evaluator_time:
                time_profile = '%.02f (%.02f%%)' % (processor.time_spent, 100 * processor.time_spent / evaluator_time)
            else:
                time_profile = ''
            nodes.append('"%(pipeline)s-%(id)s" [id="%(pipeline)s-%(id)s" label="%(name)s %(time_profile)s" color="%(color)s"];' % dict(id=processor.id, name=processor.node_name, pipeline=id(self), color=color, time_profile=time_profile))

            for consumer in self.consumers.successors(processor):
                edge_data = self.consumers.get_edge_data(processor, consumer)
                ids = dict(producer=processor.id, consumer=consumer.id, pipeline=id(self))

                if edge_data['is_error_consumer']:
                    edges.append('"%(pipeline)s-%(producer)s" -> "%(pipeline)s-%(consumer)s" [id="%(pipeline)s-%(producer)s-%(consumer)s" class="error_consumer" color="red"];' % ids)
                else:
                    edges.append('"%(pipeline)s-%(producer)s" -> "%(pipeline)s-%(consumer)s" [id="%(pipeline)s-%(producer)s-%(consumer)s" class="consumer"];' % ids)

            if not draw_dependency_edges:
                continue

            processor_dependencies = set(processor.depends_on)
            if processor_dependencies:
                for predecessor in self.get_all_processors_preceding(processor):
                    if set(predecessor.provides) & processor_dependencies:
                        ids = dict(dependee=processor.id, depended_on=predecessor.id, pipeline=id(self))
                        edges.append('"%(pipeline)s-%(dependee)s" -> "%(pipeline)s-%(depended_on)s" [id="dep-%(dependee)s-%(depended_on)s" style="dotted" constraint=false];' % ids)

        return u'digraph G {\n%s\n}' % '\n'.join(nodes + edges)

    @classmethod
    def _get_color_for_time(cls, t, mean_time):
        """ Return a yellow color for t-s close to *mean_time*, green
        when *t* is a lot lower, and red when it is a lot higher.
        """
        if not (t or mean_time):
            return '#00FF00'
        if t > mean_time:
            return '#FF%02X00' % (int(255 * mean_time / t), )
        return '#%02XFF00' % int(255 * min(1.0, t / mean_time))


class _ProcessTracer(object):
    """ Traces batons as they move from processor to processor or processor to pipeline.

    The :meth:`TwistedProcessorGraphEvaluator.traced_process` uses this class in order to create
    a list of dicts that shows each of the steps a baton has actually taken through a processor graph.

    All active tracers are invoked by :meth:`TwistedProcessorGraphEvaluator.process` and
    :meth:`TwistedProcessorGraphEvaluator._process` whenever an entry may be added to the current
    trace.

    Before a trace entry is added to the list of traced batons, the process tracer searches through the
    stack of frames in order to find its own trace token. The existence of the trace token in the
    stack indicates that it is not part of any unrelated concurrent processing, and should be traced.

    .. note:: Tracing a pipeline that also traces is currently not supported.
    
    """

    def __init__(self):
        self.traced = list()
        self.token_value = uuid.uuid4().get_hex()
        self.token_name = 'trace_token_'+self.token_value

        self.sentinel = object()
        self.encoder = util.BatonJSONEncoder()

    def refresh_trace_token(self, frame):
        """ Attempt to find and refresh the trace token in the given stack of frames.

        :return: The value of the trace token value if it is found.
        """
        if self._refresh_trace_token_if_found_in_the_current_stack(frame):
            return self.token_value

    def add_trace_entry(self, frame, kind, **state):
        """ Add an entry to the trace if the correct trace token is in the current stack of frames.

        :return: The state added to the trace.
        """
        if not self._refresh_trace_token_if_found_in_the_current_stack(frame):
            return

        if state['source'] is None:
            state['source'] = self._find_nearest_possible_source(frame)

        # encode + load is used to get a copy of the current baton that is not affected by further processing
        state['baton'] = json.loads(self.encoder.encode(state['baton']))

        if kind in ('process_source', '_process_consumer'):
            # these calls already have all information they need
            pass
        elif kind in ('process_source_raised', '_process_consumer_raised', '_process_error_consumer', '_process_error_consumer_raised'):
            state['failure'] = self._get_trace_failure()

        self._append_to_trace(**state)

        return state

    def _find_nearest_possible_source(self, frame):
        """ Returns the first 'self' variable in a stack of frames that is not part of piped.processing """
        while frame:
            possible_source = frame.f_locals.get('self', None)
            if possible_source and not isinstance(possible_source, (TwistedProcessorGraphEvaluator, failure.Failure, defer.Deferred)):
                return possible_source
            frame = frame.f_back

    def _get_trace_failure(self):
        """ Returns a dict that semi-neatly describes a failure without using much memory. """
        reason = failure.Failure()
        return dict(
            type = reason.type,
            args = reason.value.args,
            traceback = reason.getTraceback(elideFrameworkCode=True, detail='brief')
        )

    def _refresh_trace_token_if_found_in_the_current_stack(self, frame):
        """ Looks for the trace token in a stack of frames and copies it to all frames
        leading from the frame the token was found in to the given frame.

        :return: True if the token was found, False otherwise.
        """

        frames_visited = list()

        while frame:
            # It may be in the locals of the current frame
            if frame.f_locals.get(self.token_name, self.sentinel) == self.token_value:
                # if it is found, add it to the locals of the intermediary frames
                for visited_frame in frames_visited:
                    visited_frame.f_locals[self.token_name] = self.token_value

                return True

            frames_visited.append(frame)
            frame = frame.f_back

        return False

    def _append_to_trace(self, **kw):
        kw.setdefault('is_error_consumer', False)
        kw.setdefault('failure', None)
        self.traced.append(kw)


class TwistedProcessorGraphEvaluator(object):
    """ A `ProcessorGraph` evaluator that does its processing in the Twisted thread.

    * In this processor graph, processors may use deferreds.
    * Processors used in the graph are assumed to be "Twisted-safe" (non-blocking).
    """

    tracers = list()

    def __init__(self, processor_graph, name=None):
        self.processor_graph = processor_graph
        self.name = name
        self.time_spent = 0

        for processor in self.processor_graph.consumers:
            self._connect_producer_to_consumers(processor)

    @classmethod
    def _add_trace_entry(cls, *a, **kw):
        for tracer in cls.tracers:
            frame = sys._getframe(1)
            entry = tracer.add_trace_entry(frame, *a, **kw)
            if entry:
                return entry


    @classmethod
    def _refresh_trace_token(cls):
        for tracer in cls.tracers:
            frame = sys._getframe(1)
            token = tracer.refresh_trace_token(frame)
            if token:
                return token
            else:
                pass

    @defer.inlineCallbacks
    def _process(self, processor, baton, results):
        # Refresh the trace token, which will copy the trace token as far down the stack
        # as possible, so that it is easier to find by the tracer.
        # This fixes a corner case where a processor performs some deep magic that would
        # change the stack, making it hard (or even impossible) to locate the original
        # trace token location.
        self._refresh_trace_token()

        try:
            # Profile the time each processor spends.
            s = time.time()
            processed_baton = yield processor.process(baton)
            d = time.time() - s
            # Update the time the processor spends
            processor.time_spent += d
            # ... and the accumulative total.
            self.time_spent += d

            consumers = processor.get_consumers(processed_baton)
            for consumer in consumers:
                try:
                    self._add_trace_entry('_process_consumer', source=processor, destination=consumer, baton=baton, time_spent=d)
                    yield self._process(consumer, processed_baton, results)
                except Exception:
                    self._add_trace_entry('_process_consumer_raised', source=consumer, destination=processor, baton=baton)
                    raise

            if not consumers:
                # this means it is a sink, so we store the result we got from it.
                # we cannot rely on the processor being in ``self.processor_graph.sinks``
                # because the processor might have provided its own implementation
                # of :func:`.base.Processor.get_consumers`.
                self._add_trace_entry('_process_result', source=processor, destination=None, baton=processed_baton, time_spent=d)
                results.append(processed_baton)

        except Exception:
            error_consumers = processor.get_error_consumers(baton)
            if error_consumers:
                for error_consumer in error_consumers:
                    self._add_trace_entry('_process_error_consumer', source=processor, destination=error_consumer, baton=baton, is_error_consumer=True)
                    try:
                        yield self._process(error_consumer, baton, results)
                    except Exception:
                        self._add_trace_entry('_process_error_consumer_raised', source=error_consumer, destination=processor, baton=baton)
                        raise
            else:
                raise

    @defer.inlineCallbacks
    def process(self, baton):
        """ Processes a baton asynchronously through the processor graph.

        :param consume_errors: Whether to consume unhandled errors. Consumed
            exceptions are appended to the list of results.
        :returns: A list of batons, one for each sink that was encountered
            during the processing of the baton.
        """
        # collect the resulting baton from the sinks
        results = list()

        for source_processor in self.processor_graph.sources:
            # store the trace entry before processing in order to use it as a destination if the processing raises an Exception
            entry = self._add_trace_entry('process_source', source=None, destination=source_processor, baton=baton)
            try:
                yield self._process(source_processor, baton, results)
            except Exception as e:
                source = entry['source'] if entry else None
                self._add_trace_entry('process_source_raised', source=source_processor, destination=source, baton=baton)
                raise

        defer.returnValue(results)

    # Calling the evaluator directly should be the same as starting to process a baton in a pipeline:
    __call__ = process

    @defer.inlineCallbacks
    def traced_process(self, *a, **kw):
        """ Traces and processes a baton asynchronously through a processor graph.

        .. warn:: The act of tracing introduces a new variable in the locals of
            all the traced frames on the form ``trace_token_UUID4``.

        .. seealso:: :class:`_ProcessTracer` and :meth:`process`.
        """
        tracer = _ProcessTracer()
        # add the token to the current locals, so _refresh_trace_token may find it
        locals()[tracer.token_name] = tracer.token_value

        try:
            self.tracers.append(tracer)
            results = yield self.process(*a, **kw)
        except Exception:
            results = failure.Failure()
        finally:
            self.tracers.remove(tracer)
        defer.returnValue((results, tracer.traced))

    def configure_processors(self, runtime_environment):
        # give the processor a reference to its evaluator
        for processor in self.processor_graph:
            processor.evaluator = self
        self.processor_graph.configure_processors(runtime_environment)

    def _connect_producer_to_consumers(self, processor):
        """ Wire the processor to its consumers. """
        if not hasattr(processor, 'consumers'):
            e_msg = 'consumer-less processor: ' + repr(processor)
            detail = 'Did you invoke base.Processor\'s __init__?'
            raise exceptions.ConfigurationError(e_msg, detail)
        for consumer, edge_data in self.processor_graph.consumers[processor].items():
            if edge_data['is_error_consumer']:
                processor.error_consumers.append(consumer)
            else:
                processor.consumers.append(consumer)

    def __iter__(self):
        return iter(self.processor_graph)

    def __getitem__(self, item):
        return self.processor_graph[item]


class ProcessorGraphFactory(object):
    """ Takes a plugin manager and a pipeline configuration and produces a
    processor graph. """

    default_graph_evaluator = TwistedProcessorGraphEvaluator

    def __init__(self, inline_pipeline_config=Ellipsis):
        self.inline_pipeline_config = inline_pipeline_config

    def configure(self, runtime_environment):
        self.pipelines_configuration = self._get_pipeline_configuration(runtime_environment)

        if not self.pipelines_configuration:
            log.info('No pipeline definitions were found in the configuration.')

        self.plugin_manager = ProcessorPluginManager()
        self.plugin_manager.configure(runtime_environment)

        self._nest_chained_consumers()
        self._flatten_nested_pipelines()
        self._resolve_inheritance()

    def _get_pipeline_configuration(self, runtime_environment):
        if self.inline_pipeline_config is Ellipsis:
            nested_config = runtime_environment.get_configuration_value('pipelines', dict())
        else:
            nested_config = self.inline_pipeline_config

        # We're going to modify the configuration a bit, so deep-copy it as not to alter
        # the original.
        return self._flatten_pipelines_configuration(deepcopy(nested_config))

    def _is_processor_definition(self, maybe_configuration):
        # attempt to convert the configuration to a base format before checking if it is a valid configuration.
        config = self._as_processor_configuration(maybe_configuration)
        if isinstance(config, dict):
            if '__processor__' in config or 'inline-pipeline' in config:
                return True
        return False

    def _is_pipeline_definition(self, maybe_definition):
        if isinstance(maybe_definition, dict):
            for key in maybe_definition:
                # one of these keys must exist in order for a dictionary to be a pipeline definition:
                if key in ('chained_consumers', 'consumers', 'inherits'):
                    return True
            return False
        if isinstance(maybe_definition, list) and len(maybe_definition) > 0:
            return self._is_processor_definition(maybe_definition[0])
        return False

    def _flatten_pipelines_configuration(self, nested_config):
        flat_config = dict()
        for path, maybe_definition in util.dict_iterate_paths(nested_config):
            # if our immediate parent is a pipeline definition,
            parent_is_a_pipeline = path.rsplit('.', 1)[0] in flat_config

            if self._is_pipeline_definition(maybe_definition) and not parent_is_a_pipeline:
                flat_config[path] = maybe_definition

        return flat_config

    def _nest_chained_consumers(self):
        """ Nest all chained consumers in the graph configuration. """
        for pipeline_name, pipeline in self.pipelines_configuration.items():
            # default to lists on the top level of a pipeline definition being chained_consumers:
            if isinstance(pipeline, list):
                pipeline = dict(chained_consumers=pipeline)
                self.pipelines_configuration[pipeline_name] = pipeline
            try:
                if 'inherits' in pipeline:
                    self._fail_if_configuration_defines_override_and_consumers(pipeline)
                    # We deal with these in _resolve_inheritance.
                else:
                    self._nest_chained_consumers_in_pipeline(pipeline)
                    self._attach_root_error_consumers_to_root_consumers(pipeline)
            except exceptions.ConfigurationError, e:
                e.msg = 'failed to create pipeline "%s": %s' % (pipeline_name, e.msg)
                raise # keep the original stack trace.

    def _nest_chained_consumers_in_pipeline(self, pipeline):
        """ Chain processors in `chained_consumers` by adding each
        processor to its producer's list of consumers. """
        self._fail_if_pipeline_is_misconfigured(pipeline)

        # processors may be in different formats, so we deal with converting them to a
        # base format before as we make sure they are properly nested.
        for consumers_key in 'consumers', 'error_consumers', 'chained_consumers', 'chained_error_consumers':
            self._ensure_is_list_of_processors(pipeline.get(consumers_key, list()))


        for chained_consumers_type in ('chained_consumers', 'chained_error_consumers'):
            base_name = chained_consumers_type.split('_',1)[-1]
            chained_consumers = pipeline.pop(chained_consumers_type, list())

            # Make sure that any chained_consumers or chained_error_consumers becomes the first
            # consumer of their parent processor by inserting them at the first position instead of
            # appending them.
            if len(chained_consumers) == 1:
                # Trivial case
                pipeline.setdefault(base_name, []).insert(0, chained_consumers[0])
            elif len(chained_consumers) > 1:
                # Several consumers that should be chained to each other.
                consumer = self._coalesce_chained_consumers(chained_consumers)
                # Finally make the source of the chain a consumer of the pipeline.
                pipeline.setdefault(base_name, []).insert(0, consumer)

            # ... and recurse to all other processors.
            for processor in pipeline.get(base_name, []):
                self._nest_chained_consumers_in_pipeline(processor)

    def _attach_root_error_consumers_to_root_consumers(self, pipeline):
        """ Attach all the error_consumers defined on the root level to all the source consumers. """
        for error_consumer in pipeline.pop('error_consumers', list()):
            for root_consumer in pipeline.get('consumers', list()):
                root_consumer.setdefault('error_consumers', list()).append(error_consumer)


    def _as_processor_configuration(self, processor_configuration):
        """ Ensure that a processor configuration is in a base format.

        Processors can be defined in several ways, the base format being:

            __processor__: processor-name
            key1: value1
            key2: value2

        A shorthand version is also possible (identical with the above):

            processor-name:
                key1: value2
                key2: value2

        If the processor does not require any configuration, even shorter:

            processor-name

        This function takes care of rewriting the two latter examples into
        the first one, so any further operations may rely on the processor
        configurations to be in only one format.

        .. note::
            This function may change its input arguments inline before
            returning it. Do not rely on input to this function being unchanged.

        """

        if isinstance(processor_configuration, basestring):
            # if it is a string, we assume it is the name of a processor
            return dict(__processor__=processor_configuration)

        if isinstance(processor_configuration, dict):
            if '__processor__' in processor_configuration or 'inline-pipeline' in processor_configuration:
                # it is already a processor configuration
                return processor_configuration

            # find the non-built-in key and assume that is the processor configuration. the
            # built-in keys are currently either strings or lists, so we look for a dict.
            possible_configurations = [(name, config) for name, config in processor_configuration.items() if isinstance(config, dict)]
            if len(possible_configurations) != 1:
                # we have no way of telling what could be the processor configuration here,
                # so we do nothing in order to let some later step handle an eventual
                # configuration error
                return processor_configuration

            processor_name = possible_configurations[0][0]
            processor_arguments = possible_configurations[0][1]

            # delete the nested configuration
            del processor_configuration[processor_name]

            # update the processor name and configuration
            processor_configuration['__processor__'] = processor_name
            processor_configuration.update(processor_arguments)

            return processor_configuration
        return processor_configuration

    def _ensure_is_list_of_processors(self, processor_configurations):
        for i, processor_configuration in enumerate(processor_configurations):
            processor_configurations[i] = self._as_processor_configuration(processor_configuration)

    @classmethod
    def _fail_if_pipeline_is_misconfigured(cls, pipeline):
        # the pipeline configuration might not be iterable...
        if isinstance(pipeline, collections.Iterable):
            if any(key in pipeline for key in ('__processor__', 'inline-pipeline', 'chained_consumers', 'consumers', 'existing')):
                return

        e_msg = 'invalid contents of pipeline-configuration'
        detail = ('The contents of a pipeline-configuration is expected to be a dictionary, '
                  'with either "chained_consumers" or "consumers".')

        if isinstance(pipeline, list):
            hint = ('It seems like you have specified a list. Did you forget to nest it in "chained_consumers"'
                    ' or "consumers"?')
        else:
            hint = ('The relevant configuration was: %r.' % pipeline)

        raise exceptions.ConfigurationError(e_msg, detail, hint)

    @classmethod
    def _coalesce_chained_consumers(cls, chained_consumers):
        """ Take a list of consumers, chain them, and return the first
        consumer with subsequent consumers nested."""
        # Start with the last one:
        consumer = chained_consumers.pop()
        # Then keep making the last consumer consume its preceding producer.
        while chained_consumers:
            producer = chained_consumers.pop()
            producer.setdefault('consumers', []).append(consumer)
            consumer = producer
        return consumer

    def _flatten_nested_pipelines(self):
        """ Replace sub-pipelines with the processors contained in them. """
        for pipeline_name, pipeline in self.pipelines_configuration.items():
            self._flatten_pipeline(pipeline_name, pipeline)

    def _flatten_pipeline(self, pipeline_name, pipeline):
        """ Walk over all consumers and replace subpipeline-references with its
        processors. """
        if not pipeline.get('consumers'):
            return

        for attribute_to_flatten in 'consumers', 'error_consumers':
            attribute_after_flattening = []
            for attribute in pipeline.get(attribute_to_flatten, []):
                # Sub-pipelines can be nested in sub-consumers.
                self._flatten_pipeline(pipeline_name, attribute)

                if '__processor__' in attribute or 'existing' in attribute:
                    attribute_after_flattening.append(attribute)
                    # Any potential sub-pipelines in consumers have been flattened above,
                    # so we're done here.
                    continue
                else:
                    if not 'inline-pipeline' in attribute:
                        e_msg = 'invalid processor/inline-pipeline-reference'
                        detail = 'No processor/inline-pipeline among these keys: "%s"' % '", "'.join(sorted(attribute.keys()))
                        hint = 'Did you misspell something?'
                        raise exceptions.ConfigurationError(e_msg, detail, hint)

                subpipeline_name = util.resolve_sibling_import(attribute.pop('inline-pipeline'), pipeline_name)
                subpipeline = self.pipelines_configuration[subpipeline_name]

                # The referenced pipeline may have references to other pipelines.
                self._flatten_pipeline(subpipeline_name, subpipeline)

                # The pipeline may have multiple references, so we need a deepcopy.
                processors = deepcopy(subpipeline['consumers'])
                self._attach_consumers_to_sinks(processors, attribute.get('consumers', []))

                error_consumers = attribute.get('error_consumers', [])
                if error_consumers:
                    for processor in processors:
                        processor.setdefault('error_consumers', []).extend(error_consumers)
                attribute_after_flattening.extend(processors)

            if attribute_after_flattening:
                pipeline[attribute_to_flatten] = attribute_after_flattening

    def _attach_consumers_to_sinks(self, processors, consumers):
        """ Find sinks in `processors` and tack consumers to them. """
        if not consumers:
            return

        sinks = self._find_sinks_recursively(processors)
        for sink in sinks:
            for consumer in consumers:
                sink.setdefault('consumers', []).append(consumer)

    def _find_sinks_recursively(self, processors):
        """ Return processors without consumers --- i.e. sinks. """
        sinks = []
        for processor in processors:
            consumers = processor.get('consumers')
            error_consumers = processor.get('error_consumers')
            if not (consumers or error_consumers):
                # No consumers, so it's a sink.
                sinks.append(processor)
            else:
                # No sink, but it can contain one.
                if consumers:
                    sinks.extend(self._find_sinks_recursively(consumers))
                if error_consumers:
                    sinks.extend(self._find_sinks_recursively(error_consumers))
        return sinks

    def _resolve_inheritance(self):
        """ Process the configurations of pipelines that inherit from
        other pipelines.

        Base the pipeline on a deepcopy of the inherited
        pipeline. Then, walk the processor graph breadth-first and
        replace the configuration of the processors that match those
        in *overrides*.
        """
        for pipeline_name, configuration in self.pipelines_configuration.items():
            if 'inherits' not in configuration:
                continue

            self._validate_inheriting_pipeline(pipeline_name, configuration)

            name_of_parent_pipeline = configuration['inherits']
            parent_pipeline = self.pipelines_configuration[name_of_parent_pipeline]
            replacement_pipeline = deepcopy(parent_pipeline)
            # Keep a note of how the pipeline is assembled:
            replacement_pipeline['inherits'] = name_of_parent_pipeline

            self._merge_pipeline_with_overrides(replacement_pipeline, configuration['overrides'], pipeline_name)
            self.pipelines_configuration[pipeline_name] = replacement_pipeline

    @classmethod
    def _merge_pipeline_with_overrides(cls, pipeline, overrides, pipeline_name):
        """ Visit processors in *pipeline* breadth first. *overrides*
        provides a queue of processor configurations that should
        override the corresponding ones in *pipeline*.

        If *overrides* is not empty when all processors have been
        visited, a `ConfigurationError` is raised.
        """
        if not overrides:
            return

        # Visit processors breadth first.
        processor_queue = collections.deque(pipeline['consumers'])
        overrides = collections.deque(overrides)
        while processor_queue:
            if not overrides:
                break

            current_processor = processor_queue.popleft()
            processor_queue.extendleft(current_processor.get('consumers', tuple()))
            processor_queue.extendleft(current_processor.get('error_consumers', tuple()))

            next_in_queue = overrides[0] # Peek
            if current_processor['__processor__'] == next_in_queue['__processor__']:
                cls._override_processor_options(current_processor, overrides.popleft())

        cls._fail_if_not_all_overrides_were_used(overrides, pipeline_name)

    @classmethod
    def _override_processor_options(cls, processor_options, override_options):
        """ Override the options in *processor_options* by those in *override_options*.

        Keys present in *processor_options* but not in *override_options* are
        removed, with the exception of *consumers* --- since that
        contains processors that consume this one.
        """
        keys_to_keep = set(override_options.keys() + ['consumers', 'error_consumers'])
        for key in processor_options.keys():
            if key not in keys_to_keep:
                del processor_options[key]

        for key, value in override_options.items():
            processor_options[key] = value

    @classmethod
    def _fail_if_configuration_defines_override_and_consumers(cls, configuration):
        if not (('consumers' in configuration or 'error_consumers' in configuration) and 'inherits' in configuration):
            return

        e_msg = 'cannot define "consumers" in overriding configuration'
        detail = ('An overriding configuration should only define overriding options --- '
                  'it cannot change the structure of the processor graph. '
                  'Overriding configuration follows: %s' % (pprint.pformat(configuration)))
        raise exceptions.ConfigurationError(e_msg, detail)

    def _validate_inheriting_pipeline(self, pipeline_name, configuration):
        """ Ensure that the referred-to pipeline actually exists, and
        that it does not in turn inherit a pipeline. """
        name_of_parent_pipeline = configuration['inherits']
        parent_pipeline = self.pipelines_configuration.get(name_of_parent_pipeline)
        if not parent_pipeline:
            e_msg = 'no such pipeline: ' + name_of_parent_pipeline
            detail = ('The pipeline "%(pipeline)s" tried to inherit the nonexisting pipeline "%(parent)s".' %
                      dict(pipeline=pipeline_name, parent=name_of_parent_pipeline))
            raise exceptions.ConfigurationError(e_msg, detail)

        parents_inheritor = self.pipelines_configuration[name_of_parent_pipeline].get('inherits')
        if parents_inheritor is not None:
            e_msg = 'tried to inherit from pipeline that already inherits'
            detail = (('The pipeline "%(pipeline)s" wants to inherit from "%(parent)s", which already inherits from'
                       ' "%(grandparent)s". Inheritance hierarchies cannot be deeper than 1 at the moment.') %
                      dict(pipeline=pipeline_name, parent=name_of_parent_pipeline, grandparent=parents_inheritor))
            raise exceptions.ConfigurationError(e_msg, detail)

    @classmethod
    def _fail_if_not_all_overrides_were_used(cls, overrides, pipeline_name):
        if not overrides:
            return

        names_of_processors = [override['__processor__'] for override in overrides]
        e_msg = 'overrides provided but not used: %s' % (names_of_processors, )
        detail = ('The pipeline "%s" was provided with overrides to processors not found in the pipeline it '
                  'inherited from.') % (pipeline_name, )
        raise exceptions.ConfigurationError(e_msg, detail)

    def _ensure_configuration_keys_are_ascii(self, processor_configuration):
        ascii_processor_configuration = dict()
        for key, value in processor_configuration.items():
            if isinstance(key, unicode):
                try:
                    key = key.encode('ascii')
                except UnicodeEncodeError:
                    e_msg = 'Processor configuration keys must conform to ASCII.'
                    raise exceptions.ConfigurationError(e_msg)
            ascii_processor_configuration[key] = value
        return ascii_processor_configuration

    def _make_processor(self, processor_configuration):
        """ Return a processor instance provided its configuration. """
        assert not 'inline-pipeline' in processor_configuration, "Unflattened configuration provided"

        if 'existing' in processor_configuration:
            # Placeholder we'll replace when all processors have been made.
            return processor_configuration['existing']

        # We're going to modify the configuration, so don't modify the original.
        # Deep-copying with all the consumers is an expensive operation, so temporarily
        # remove them from the configuration:
        consumers = processor_configuration.pop('consumers', None)
        error_consumers = processor_configuration.pop('error_consumers', None)

        copied_processor_configuration = deepcopy(processor_configuration)

        # add the consumers and error_consumers back into the original configuration:
        if consumers is not None:
            processor_configuration['consumers'] = consumers
        if error_consumers is not None:
            processor_configuration['error_consumers'] = error_consumers

        # We pop away stuff because we want the remaining dictionary to define the
        # configuration values.
        plugin_name = copied_processor_configuration.pop('__processor__')
        processor_id = copied_processor_configuration.pop('id', None)

        plugin_factory = self._get_plugin_factory_or_fail(plugin_name)
        try:
            processor = plugin_factory(**self._ensure_configuration_keys_are_ascii(copied_processor_configuration))
        except TypeError, e:
            # If creating the processor blows up, the stacktrace isn't
            # particularly helpful, so put some effort in making a
            # helpful exception.
            self._explain_instantiation_typeerror(e, plugin_name, plugin_factory, copied_processor_configuration)

        processor.processor_configuration = copied_processor_configuration
        processor.id = processor_id

        return processor

    def _explain_instantiation_typeerror(self, type_error, plugin_name, plugin_factory, processor_configuration):
        message = type_error.args[0]
        if '__init__() got an unexpected keyword argument' in message:
            unknown_argument = message.split("'")[1]
            e_msg = 'invalid keyword "%s" for processor: %s' % (unknown_argument, plugin_name)
            detail = ('The processor was configured with the argument "%(argument)s", which is unknown. \n'
                      'Note that configuration options are passed directly to "%(factory)s"\'s __init__.')
            detail = detail % dict(argument=unknown_argument, factory=plugin_factory)
            hint = 'Check its spelling and/or check the documentation of "%s"' % (plugin_factory, )
        elif '__init__() takes exactly' in message:
            e_msg = 'insufficient configuration of processor: %s' % (plugin_name, )
            argspec = inspect.getargspec(plugin_factory.__init__)
            expected_args = sorted(argspec.args[1:]) # 'self' is the first element
            detail = 'Missing arguments: %s\n\nProvided configuration: %s' % (expected_args,
                                                                              pprint.pformat(processor_configuration))
            hint = None
        elif '__init__() takes at least' in message:
            e_msg = 'insufficient configuration of processor: %s' % (plugin_name, )
            argspec = inspect.getargspec(plugin_factory.__init__)
            # remove arguments with defaults
            arglen = len(argspec.args or [])
            defaultslen = len(argspec.defaults or [])
            expected_args = sorted(argspec.args[1:arglen - defaultslen])
            detail = ('The processor %(plugin_name)s expects one or more of the following arguments: "%(expected_args)s", '
                      'but the configuration provided "%(provided_args)s".')
            detail = detail % dict(plugin_name=plugin_name,
                                   expected_args='", "'.join(expected_args),
                                   provided_args='", "'.join(processor_configuration.keys()))
            hint = None
        else:
            log.error('Error when creating plugin %s' % (plugin_name, ))
            raise
        raise exceptions.ConfigurationError(e_msg, detail, hint)

    def _make_processors_and_wire_relations(self, processor_configuration, builder, existing_processors_by_id, is_error_consumer=False):
        """ Takes a processor configuration and instantiates the processor and its
        consumers, while also wiring up the producer-consumer-relationships.

        `builder` is a `ProcessorGraphBuilder`-object, upon which we'll tack consumers.
        """
        processor = self._make_processor(processor_configuration)
        if 'id' in processor_configuration:
            existing_processors_by_id[processor_configuration['id']] = processor

        builder = builder.add_processor(processor, is_error_consumer)

        for consumer in processor_configuration.get('consumers', []):
            self._make_processors_and_wire_relations(consumer, builder, existing_processors_by_id, is_error_consumer=False)

        for consumer in processor_configuration.get('error_consumers', []):
            self._make_processors_and_wire_relations(consumer, builder, existing_processors_by_id, is_error_consumer=True)

    def _replace_references_to_existing_with_processors(self, pg, existing_processors_by_id):
        # Replace all references to existing processors with references to the actual processor.
        for processor_reference in pg.consumers.nodes():
            if processor_reference not in existing_processors_by_id:
                # Just a regular node, so nothing to do.
                continue

            # For every node that's pointing to the existing processor
            for predecessor in pg.consumers.predecessors(processor_reference):
                # ... make it point to the real processor
                edge_data = pg.consumers.get_edge_data(predecessor, processor_reference)
                pg.consumers.replace_edge_from(predecessor, processor_reference, existing_processors_by_id[processor_reference], **edge_data)
            pg.consumers.remove_node(processor_reference)

        # Remove all references from sources and sinks. These have
        # been replaced with their actual nodes by now.
        pg.sources = [source for source in pg.sources if not isinstance(source, basestring)]
        pg.sinks = [sink for sink in pg.sinks if not isinstance(sink, basestring)]

        # Since previously "disconnected" processors can have been
        # connected, check that the sinks *still* are sinks.
        for i, node in enumerate(pg.sinks):
            # ... and a sink should not have any consumers
            if pg.consumers.successors(node):
                del pg.sinks[i]

    def make_processor_graph(self, pipeline_name):
        """ Make a processor graph of the provided pipeline.

        :param collector: If provided, the output of all sinks will be
            appended to this list-like.
        """
        assert pipeline_name in self.pipelines_configuration, "No pipeline '%s'." % pipeline_name

        existing_processors_by_id = dict()

        pg = ProcessorGraph()
        builder = pg.get_builder()

        for consumer in self.pipelines_configuration[pipeline_name]['consumers']:
            self._make_processors_and_wire_relations(consumer, builder, existing_processors_by_id)

        self._replace_references_to_existing_with_processors(pg, existing_processors_by_id)

        self._annotate_configuration_anomalies(pg, pipeline_name)
        return pg

    def make_evaluator(self, pipeline_name, evaluator_factory=None):
        """ Make a processor graph evaluator of the provided pipeline.

        :param collector: If provided, the output of all sinks will be
            appended to this list-like.
        """
        pg = self.make_processor_graph(pipeline_name)

        if evaluator_factory:
            return evaluator_factory(pg, pipeline_name)
        return self.default_graph_evaluator(pg, pipeline_name)

    def _fail_unknown_evaluator_kind(self, kind):
        msg = 'Unknown evaluator kind: %s.' % kind
        raise exceptions.ConfigurationError(msg)

    def make_all_pipelines(self, included_pipelines=None, evaluator_factory=None):
        """ Instantiate all configured pipelines and return a
        dictionary mapping pipeline names to the corresponding
        evaluator.

        If *included_pipelines* is specified, only those pipelines are
        created. If *None*, which is the default, all pipelines are
        created.
        """
        processor_by_pipeline_name = dict()

        pipelines_to_make = included_pipelines or self.pipelines_configuration.keys()
        for pipeline_name in pipelines_to_make:
            try:
                processor = self.make_evaluator(pipeline_name, evaluator_factory=evaluator_factory)
                processor_by_pipeline_name[pipeline_name] = processor
            except exceptions.ConfigurationError, e:
                e_msg = 'failed to create pipeline "%s": %s' % (pipeline_name, e.msg)
                raise exceptions.ConfigurationError(e_msg, e.detail, e.hint)

        return processor_by_pipeline_name

    def _get_plugin_factory_or_fail(self, plugin_name):
        """ Returns a factory that produces plugins by *plugin_name*,
        or raises a :exc:`Misconfiguration` exception. """
        plugin_factory = self.plugin_manager.get_plugin_factory(plugin_name)
        if plugin_factory:
            return plugin_factory

        e_msg = 'no such plug-in: ' + plugin_name
        detail = """ While creating a processor-graph, a processor
        referencing an unknown plug-in was encountered. """
        hint = """ If you have not simply misspelled, ensure that the
        plug-in-manager is configured to load the package containing
        your plug-in, that the plug-in has a name-variable, and that
        it is implementing the right interface(s). """
        raise exceptions.InvalidConfigurationError(e_msg, detail, hint)

    def _annotate_configuration_anomalies(self, processor_graph, pipeline_name):
        processors = processor_graph.consumers.nodes()

        anomalies = []
        for processor in processors:
            if not processor.depends_on:
                # No dependencies, no problems.
                continue

            provided_so_far = set()
            # Naive algorithm that isn't particularly efficient, but
            # it works even with cycles, etc. This is not a
            # performance critical path anyway.
            for preceding_processor in processor_graph.get_all_processors_preceding(processor):
                provided_so_far.update(preceding_processor.provides)
                provided_so_far.update(getattr(preceding_processor, 'instance_provides', list()))

            instance_depends_on = getattr(processor, 'instance_depends_on', list())
            missing_dependencies = set(processor.depends_on + instance_depends_on) - provided_so_far
            if not missing_dependencies:
                continue

            anomaly, anomaly_warning = self._describe_anomaly(processor, missing_dependencies, pipeline_name)
            anomalies.append(anomaly)
            if not processor_graph.suppress_warnings:
                warnings.warn(anomaly_warning)

        processor_graph.configuration_anomalies = anomalies

    def _describe_anomaly(self, processor, missing_dependencies, pipeline_name):
        # Determine which plugins provide the missing dependencies
        providers = dict()
        suggested_missing_processors = dict()
        for keyword in missing_dependencies:
            plugin_names = self.plugin_manager.get_providers_of_keyword(keyword)
            providers[keyword] = plugin_names
            for name_of_plugin in plugin_names:
                suggested_missing_processors.setdefault(name_of_plugin, []).append(keyword)

        anomaly = dict(processor=processor.name, configuration=processor.processor_configuration,
                       missing_dependencies=missing_dependencies, providers=providers)

        # Prepare a ConfigurationWarning. It's not up to this method whether to raise it, though.
        missing_dependencies = ', '.join(sorted(missing_dependencies))
        w_msg = u'processor with unsatisfied dependencies: "%s" in pipeline "%s"' % (processor.name, pipeline_name)
        detail = 'These keywords are not provided by preceding processors: ' + missing_dependencies

        processor_suggestions = []
        for processor_name, missing_keywords in suggested_missing_processors.items():
            processor_suggestions.append(u'- %s (providing %s)' % (processor_name, ','.join(missing_keywords)))
        if processor_suggestions:
            hint = u'Are you missing any of the following processors? \n\n' + '\n\n'.join(processor_suggestions)
        else:
            hint = (u'Furthermore, no plugins were found that provide those keywords. Are you loading the right '
                    'plugins from the right modules?')
        anomaly_warning = exceptions.ConfigurationWarning(w_msg, detail, hint)

        return anomaly, anomaly_warning


class RuntimeEnvironment(object):
    """ The runtime environment is used to configure configurables
    like providers, processors and so on.

    :ivar application: An instance of :class:`twisted.application.service.Application`.
    :ivar configuration_manager:  See :class:`.conf.ConfigurationManager`.
    :ivar dependency_manager: See :class:`.dependencies.DependencyManager`.
    :ivar resource_manager: See :class:`.resource.ResourceManager`.
    """

    def __init__(self):
        self.application = service.Application('piped')
        self.configuration_manager = conf.ConfigurationManager()
        self.resource_manager = resource.ResourceManager()
        self.dependency_manager = dependencies.DependencyManager()

    def configure(self):
        for configurable in (self.configuration_manager, self.resource_manager, self.dependency_manager):
            configurable.configure(self)

    def get_configuration_value(self, *a, **kw):
        """ See :func:`piped.conf.ConfigurationManager.get`. """
        return self.configuration_manager.get(*a, **kw)

    def create_dependency_map(self, *a, **kw):
        """ See :func:`piped.dependencies.DependencyManager.create_dependency_map`. """
        return self.dependency_manager.create_dependency_map(*a, **kw)


class ProcessorPluginManager(plugin.PluginManager):
    plugin_packages = [piped_processors, plugins]
    plugin_interface = IProcessor
    plugin_configuration_name = 'processors'

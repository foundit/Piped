# Copyright (c) 2010-2011, Found IT A/S and Piped Project Contributors.
# See LICENSE for details.
from twisted.internet import defer
from twisted.python import filepath, reflect
from zope import interface

from piped import util, processing
from piped.processors import base


class DiagramTrace(base.InputOutputProcessor):
    """ Create a dot-graph of trace results. """
    interface.classProvides(processing.IProcessor)
    name = 'diagram-trace'

    def __init__(self, input_path='trace', output_path='dot', show_unused_processors=True, **kw):
        """
        :param input_path: The path to the trace in the baton.
        :param output_path: The path to save the dot graph to.
        :param show_unused_processors: Whether to show unused processors in the dot graph. Defaults to true.
        """
        super(DiagramTrace, self).__init__(input_path=input_path, output_path=output_path, **kw)

        self.show_unused_processors = show_unused_processors

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

    def configure(self, runtime_environment):
        dm = runtime_environment.dependency_manager
        self.pipeline_provider_dependency = dm.add_dependency(self, dict(provider='pipeline_provider'))

    def process_input(self, input, baton):
        processors_by_pipeline, unknowns = self._find_processors_and_unknowns(input)
        total_time, mean_time = self._calculate_mean_and_total_time(input)

        graph_list = ['digraph G { labeljust=l \n']

        # Add the unknown nodes to the graph:
        for unknown in unknowns:
            graph_list.append('%s [label="%s"]' % (id(unknown), getattr(unknown, 'name', repr(unknown))))

        # Render return values as unique nodes with a diamond shape
        for trace_step in input:
            if not trace_step['destination']:
                graph_list.append('%s [label="", shape="diamond"]' % id(trace_step['baton']))

        # Create subgraphs for each pipeline in the trace.
        for pipeline, processors in processors_by_pipeline.items():
            subgraph_list = self._create_pipeline_subgraph(pipeline, processors)

            graph_list.append('\n'.join(subgraph_list))
            graph_list.append('\n')

        # Add the traced edges to the graph
        for i, trace_step in enumerate(input):
            traced_edge = self._create_traced_edge_entry(i, trace_step, mean_time)
            graph_list.append(traced_edge)
            graph_list.append('\n')

        graph_list.append('}\n')
        return '\n'.join(graph_list)

    def _calculate_mean_and_total_time(self, trace):
        total_time = 0
        steps_with_time = 0

        for trace_step in trace:
            if 'time_spent' in trace_step:
                total_time += trace_step['time_spent']
                steps_with_time += 1

        mean_time = total_time/max(steps_with_time, 1)

        return total_time, mean_time

    def _find_processors_and_unknowns(self, trace):
        processors_by_pipeline = dict()
        unknowns = set()

        for trace_step in trace:
            for maybe_processor in trace_step['source'], trace_step['destination']:
                if not maybe_processor:
                    # it may be None, which is a return value
                    continue

                if isinstance(maybe_processor, base.Processor):
                    processors_by_pipeline.setdefault(maybe_processor.evaluator, set()).add(maybe_processor)
                else:
                    unknowns.add(maybe_processor)

        return processors_by_pipeline, unknowns

    def _create_pipeline_subgraph(self, pipeline, processors):
        subgraph_list = ['subgraph "cluster%s" { label="%s"; ' % (pipeline.name, pipeline.name)]

        for processor in processors:
            subgraph_list.append('%s [label="%s"]' % (id(processor), processor.node_name) +'\n')

        if self.show_unused_processors:
            used_processors = set(processors)
            all_processors = set([processor for processor in pipeline])
            unused_processors = all_processors.difference(used_processors)

            for processor in used_processors:
                # find unused consumers of all used processors
                for consumer in processor.consumers:
                    if consumer in unused_processors:
                        # create a gray edge between the used processor and the unused consumer
                        subgraph_list.append('%s -> %s [color="gray", fontcolor="gray", style="dotted"]' % (id(processor), id(consumer)))

                # .. and do the same with the error consumers, but use pink as the color
                for error_consumer in processor.error_consumers:
                    if error_consumer in unused_processors:
                        subgraph_list.append('%s -> %s [color="pink", fontcolor="pink", style="dotted"]' % (id(processor), id(error_consumer)))

            for processor in unused_processors:
                # this processor is unused, so render it as a dotted gray node
                subgraph_list.append('%s [label="%s", fontcolor="gray" style="dotted"]' % (id(processor), processor.name) +'\n')

                # since we didn't use this processor, we didn't use its consumers ...
                for consumer in processor.consumers:
                    subgraph_list.append('%s -> %s [color="gray", fontcolor="gray", style="dotted"]' % (id(processor), id(consumer)))

                # ... nor its error consumers:
                for error_consumer in processor.error_consumers:
                    subgraph_list.append('%s -> %s [color="pink", fontcolor="pink", style="dotted"]' % (id(processor), id(error_consumer)))

        subgraph_list.append('}\n')

        return subgraph_list

    def _create_traced_edge_entry(self, i, trace_step, mean_time):
        time_spent = trace_step.get('time_spent', None)

        color = self._get_color_for_time(time_spent, mean_time) if time_spent is not None else 'black'
        fontcolor = 'black'
        constraint = 'true'
        style = 'solid'

        # if this edge is used because of an exception, we render it a bit differently:
        if trace_step.get('failure') is not None:
            style = 'dashed'
            if trace_step.get('is_error_consumer', False):
                color = 'red'
                fontcolor = 'red'
            else:
                color = 'pink'
                # we dont want unhandled errors to affect the layout of the graph.
                constraint = 'false'

        source_id = id(trace_step['source'])
        destination_id = id(trace_step['destination'])

        if not trace_step['destination']:
            # if the destination is None, it is a return value, and we use the copied batons id instead of id(None).
            destination_id = id(trace_step['baton'])

        label = '%i' % (i+1)
        tooltip = ''
        if time_spent:
            tooltip = '%.2f ms' %(time_spent*1000)

        return '%s -> %s [label="%s", id="baton_%i", color="%s", fontcolor="%s", constraint=%s, style="%s", tooltip="%s", labeltooltip="%s"]' % (source_id, destination_id, label, i, color, fontcolor, constraint, style, tooltip, tooltip)


class RenderTrace(base.InputOutputProcessor):
    """ Creates a HTML-page containing the results of a rendered trace. """
    interface.classProvides(processing.IProcessor)
    name = 'render-trace'

    def __init__(self, request_path='request', svg_path='dot', input_path='trace', output_path='content', skip_if_nonexistent=False, template=None, json_encoder=None, **kw):
        """
        :param request_path: Path to the web request in the baton.
        :param svg_path: Path to the rendered svg.
        :param input_path: Path to the trace in the baton.
        :param output_path: Path to write the html to in the baton.
        :param skip_if_nonexistent: Whether to skip this processor if there is no trace. Defaults to False, as we will render
            the necessary HTML form even if there is no trace.
        :param template: Name of the html template to use.
        :param json_encoder: Fully qualified name of the json encoder to use.
        """
        super(RenderTrace, self).__init__(input_path=input_path, output_path=output_path, skip_if_nonexistent=skip_if_nonexistent, **kw)

        self.request_path = request_path
        self.svg_path = svg_path

        self.template = util.expand_filepath(template or filepath.FilePath(__file__).sibling('data').child('trace_template.html'))
        self.json_encoder_name = json_encoder or reflect.fullyQualifiedName(util.BatonJSONEncoder)

    def configure(self, runtime_environment):
        dm = runtime_environment.dependency_manager
        self.pipeline_provider_dependency = dm.add_dependency(self, dict(provider='pipeline_provider'))

        self.json_encoder = reflect.namedAny(self.json_encoder_name)()

    @defer.inlineCallbacks
    def process_input(self, input, baton):
        trace = input or list()
        pipeline_provider = yield self.pipeline_provider_dependency.wait_for_resource()

        dot = util.dict_get_path(baton, self.svg_path, '')

        steps = list()
        processors = dict()

        for trace_step in trace:
            steps.append(
                dict(
                    baton = trace_step['baton'],
                    failure = trace_step.get('failure'),
                    source = id(trace_step['source']),
                    destination = id(trace_step['destination']),
                )
            )

            for processor in trace_step['source'], trace_step['destination']:
                if not processor:
                    continue

                # store the current state of the processor
                processors[id(processor)] = dict(name=reflect.fullyQualifiedName(processor.__class__), state=getattr(processor, '__dict__', dict()))
        
        pipeline_names = sorted(pipeline_provider.processor_graph_factory.pipelines_configuration.keys())

        variables = dict(dot=dot, steps=self.json_encoder.encode(steps), pipeline_names=self.json_encoder.encode(pipeline_names), processors=self.json_encoder.encode(processors))

        # since this is not a performance critical path at all, we read the template from disk every single time so that the template
        # can be edited without restarting the piped service.
        trace_template = open(self.template).read()
        html = trace_template % variables

        defer.returnValue(html)
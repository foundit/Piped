import logging
import os

from piped import service, util, exceptions
from twisted.internet import defer
from zope import interface

import psutil

from piped_statsd import client


logger = logging.getLogger(__name__)


class MetricReporter(object):
    """ Metric reporter that is invoked periodically by the MetricsReporter-service.

    Metrics should be prefixed with `prefix` and tagged with `tags`. The clients are
    available through "statsd".
    """

    def __init__(self, prefix, statsd, client_name='default', tags=None):
        self.prefix = prefix
        self.statsd = getattr(statsd, client_name)
        self.tags = tags

    def __call__(self):
        raise NotImplementedError()


class ProcessInfoReporter(MetricReporter):

    def __init__(self, *a, **kw):
        self.values = set(kw.pop('values', []))
        super(ProcessInfoReporter, self).__init__(*a, **kw)
        self.process = psutil.Process(os.getpid())

        for value in self.values:
            if not hasattr(self, 'report_' + value):
                raise exceptions.ConfigurationError('unknown process value: [{0}]'.format(value))

    def __call__(self):
        for value in self.values:
            getattr(self, 'report_' + value)()

    def report_connections(self):
        value = len(self.process.get_connections())
        self.statsd.gauge(self.prefix + '.connections', value, tags=self.tags)

    def report_open_files(self):
        value = len(self.process.get_open_files())
        self.statsd.gauge(self.prefix + '.open_files', value, tags=self.tags)

    def report_memory(self):
        memory_info = self.process.get_memory_info()
        self.statsd.gauge(self.prefix + '.rss', memory_info.rss, tags=self.tags)
        self.statsd.gauge(self.prefix + '.vms', memory_info.vms, tags=self.tags)

    def report_cpu_percent(self):
        self.statsd.gauge(self.prefix + '.cpu_percent', self.process.get_cpu_percent(), tags=self.tags)

    def report_threads(self):
        self.statsd.gauge(self.prefix + '.threads', len(self.process.get_threads()), tags=self.tags)


class MetricsReporter(service.PipedService):
    """ Service that periodically lets reporters send their gauges to statsd.

    Assumes the service configuration has "metrics.enabled" being true.

    Metrics are prefixed with "metrics.prefix" every "metrics.interval" seconds
    (defaults to 5) and with "metrics.tags" as tags.

    "metrics.reporters" should be a list of reporter-configurations. The only required
    value in a reporter-configuration is "reporter", which is the name of the reporter.
    Any other values are passed to the reporter. "client_name" can be set to use a
    specific statsd-client.

    Available reporters:

        - "process". Takes "values" as a list of values to report, with possible values
        being "cpu_percent", "connections", "open_files", "memory" and "threads".

    """
    interface.classProvides(service.IPipedService)

    _standard_reporters = dict(
        process=ProcessInfoReporter
    )

    def configure(self, runtime_environment):
        service.PipedService.configure(self, runtime_environment)
        if not self.is_enabled():
            return

        metric_configuration = runtime_environment.get_configuration_value('metrics', dict())

        self.prefix = metric_configuration.get('prefix', '')
        self.tags = metric_configuration.get('tags', [])
        self.interval = metric_configuration.get('interval', 5)

        self.statsd = client.StatsdManager()
        self.statsd.configure(runtime_environment)

        self._reporters = []

        for reporter_configuration in metric_configuration.get('reporters', list()):
            if isinstance(reporter_configuration, dict):
                Reporter = self._standard_reporters.get(reporter_configuration.pop('reporter'))
            else:
                Reporter = self._standard_reporters.get(reporter_configuration)
                reporter_configuration = dict()

            try:
                self.add_reporter(Reporter(self.prefix, self.statsd, tags=self.tags, **reporter_configuration))
            except Exception:
                logger.exception('Error making reporter')
                raise exceptions.ConfigurationError('invalid reporter configuration: [{0!r}]'.format(reporter_configuration))

    def is_enabled(self):
        return self.runtime_environment.get_configuration_value('metrics.enabled', False)

    @defer.inlineCallbacks
    def run(self):
        while self.running and self.is_enabled():
            for reporter in self._reporters:
                try:
                    reporter()
                except Exception as e:
                    logger.exception('unhandled exception for reporter: [{0!r}]'.format(reporter))

            yield util.wait(self.interval)

    def add_reporter(self, reporter):
        self._reporters.append(reporter)

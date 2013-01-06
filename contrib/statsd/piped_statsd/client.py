import time

import statsd


class PipedStatsd(statsd.DogStatsd):

    def time_deferred(self, d, metric, tags, sample_rate=1):
        def _time_and_passthrough(result, start):
            self.timing(metric, time.time() - start, tags=tags, sample_rate=sample_rate)
            return result

        d.addBoth(_time_and_passthrough, time.time())
        return d


class StatsdManager(object):
    """ Returns `PipedStatsd`-clients as attributes.

    Assuming statsd-configurations under "statsd" map logical names to `PipedStatsd`-configurations, which
    currently only takes "host" and "port" as options, the manager will return `PipedStatsd`-clients with the
    corresponding configuration.

    The clients are accessible as manager_instance.logical_name.

    If a client does not have a configuration, it defaults to `host="localhost"` and "port=8125".

    Also provides the statsd-functions of the "default"-client directly. i.e. `manager_instance.gauge(...)` will
    use the "default"-client, however it is configured.
    """

    _statsd_keywords = set(["decrement", "gauge", "histogram", "increment", "set", "timed", "time_deferred", "timing"])

    def __init__(self):
        self._client_by_name = dict()

    def configure(self, runtime_environment):
        self.runtime_environment = runtime_environment
        self.default_config = runtime_environment.get_configuration_value('statsd.default', dict(host='localhost', port=8125))
        self.default = PipedStatsd(**self.default_config)

    def __getattr__(self, item):
        if item in self._statsd_keywords:
            return getattr(self.default, item)

        if item not in self._client_by_name:
            config = self.runtime_environment.get_configuration_value('statsd.{0}'.format(item), Ellipsis)
            if config is not Ellipsis:
                self._client_by_name[item] = PipedStatsd(**config)
            else:
                self._client_by_name[item] = self.default

        return self._client_by_name[item]

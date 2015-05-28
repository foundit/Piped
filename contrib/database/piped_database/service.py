import logging
import json
import os

from twisted.internet import defer

from piped import service, util


logger = logging.getLogger('piped_database.service')


class PostgresListenerService(service.PipedDependencyService):
    """Service that listens to certain channels and processes
    them.

    `channels` is assumed to be an iterable of a channel-names
    that will be LISTEN-ed on in Postgres.

    The handler is determined by `get_handler`, which is
    `handle_{channel}` by default. The handler, if any, will be
    invoked with the notification's payload, which by default is
    assumed to be JSON.

    If `lock_name` is specified, the service will not start listening
    to events or process them unless the corresponding advisory lock
    is held.

    """

    lock_name = None
    channels = []

    def configure(self, runtime_environment):
        super(PostgresListenerService, self).configure(runtime_environment)
        if not self.is_enabled():
            return

        if not getattr(self, 'listener_dependency', None):
            raise RuntimeError('A listener_dependency must be provided')
        self.listener = None

        self.waiter = util.BackoffWaiter()

    def wait(self):
        return self.waiter.wait()

    def is_enabled(self):
        return True

    @defer.inlineCallbacks
    def run_with_dependencies(self):
        if not self.is_enabled():
            return

        try:
            self.listener = yield self.cancellable(self.listener_dependency.wait_for_resource())

            if self.lock_name:
                yield self.cancellable(self.listener.wait_for_advisory_lock(self.lock_name))

            yield self.cancellable(self.run_as_leader())

        except defer.CancelledError:
            return

        except Exception as e:
            logger.exception('unhandled exception')

        finally:
            self.listener.release_lock(self.lock_name)
            yield self.wait()

    @defer.inlineCallbacks
    def run_as_leader(self):
        logger.info('Running as leader for service [{0}]. pid: [{1}]'.format(self.service_name, os.getpid()))

        try:
            notification_queue = yield self.cancellable(self.listener.listen(self.channels))

            yield self.cancellable(self.process_initial())

            while self.running:
                event = yield self.cancellable(notification_queue.get())

                handler = self.get_handler(event.channel)
                if not handler:
                    logger.warn('no handler for event [{0}]'.format(event.channel))
                    continue

                try:
                    payload = self.get_payload(event)
                except (ValueError, TypeError):
                    continue

                try:
                    result = yield handler(payload)
                except defer.CancelledError:
                    raise
                except Exception as e:
                    logger.exception('unhandled exception in run_as_leader')

        except defer.CancelledError:
            pass

        finally:
            self.listener.unlisten(notification_queue)

    def process_initial(self):
        """ Invoked before processing notifications. """

    def get_handler(self, channel):
        return getattr(self, 'handle_' + channel, None)

    def get_payload(self, event):
        return json.loads(event.payload)
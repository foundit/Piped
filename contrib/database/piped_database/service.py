import datetime
import heapq
import json
import logging
import random

import sqlalchemy as sa
from piped import exceptions, service, util
from piped import service as piped_service
from twisted.internet import defer, reactor


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

        while self.running:
            try:
                self.listener = yield self.cancellable(self.listener_dependency.wait_for_resource())

                if self.lock_name:
                    yield self.cancellable(self.listener.wait_for_advisory_lock(self.lock_name))
                
                yield self.run_as_leader()

            except defer.CancelledError:
                break

            except Exception as e:
                logger.exception('unhandled exception')

            finally:
                self.listener.release_lock(self.lock_name)
                yield self.wait()

    @defer.inlineCallbacks
    def run_as_leader(self):
        logger.info('Running as leader for service [{0}]'.format(self.service_name))
        notification_queue = yield self.listener.listen(self.channels)

        yield self.process_initial()

        try:
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


class SerialPostgresListenerService(PostgresListenerService):
    """Like PostgresListenerService, but invokes handlers for
    notifications strictly in order of transaction IDs.

    This comes with caveats, some of which are described above --- but
    the approach is nice if you need to keep things in sync, as resync
    can be done reliably wrt. to the highest txid on the other end.

    Expects notification payloads to contain `txid` and `txid_min`,
    where `txid_min` is the current minimum visible transaction.

    Long-running transactions will block the processing of subsequent
    events. Note that this is true regardless of which database the
    transaction is happening in, as transaction IDs are global for a
    Postgres-cluster. Therefore, be careful using this approach if you
    cannot control long running transactions.

    Queued notifications are kept in memory, without any bounds. Thus,
    a long-running transaction blocking a large number of other
    notifications will cause memory issues.

    """

    @defer.inlineCallbacks
    def run_as_leader(self):
        logger.info('Running as leader for service [{0}]'.format(self.service_name))
        try:
            notification_queue = yield self.listener.listen(self.channels)

            yield self.process_initial()
        
            notify_queue = []
            listener = self.listener
            
            while self.running:
                if notify_queue:
                    # If we have events we have not emitted yet, we can't just hope for another notification,
                    # we must also poll for txid --- because the long-running transaction that is blocking us
                    # can be totally unrelated, and thus not result in any notification. It can take a long
                    # time until another notification comes. Thus, poll a bit - and when we're safe, flush
                    # the queue up to the new threshold.

                    # TODO: We can piggyback on notifications as well,
                    # but then we'll have to consider requeuing it and
                    # so on. Keep it simple for now by going into polling-mode when waiting.

                    current_minimum_txid = yield self.cancellable(
                        listener.wait_for_txid_min(notify_queue[0][0])
                    )
                else:
                    notification = yield self.cancellable(notification_queue.get())

                    try:
                        payload = self.get_payload(notification)
                    except (ValueError, TypeError):
                        continue
                    
                    notification = (notification.channel, payload)

                    try:
                        current_minimum_txid = payload['txid_min']
                        heapq.heappush(notify_queue, (payload['txid'], notification))
                    except KeyError:
                        logger.error('invalid notification without txids received: [{0}]'.format(notification))

                while self.running and notify_queue and notify_queue[0][0] <= current_minimum_txid:
                    try:
                        notification = heapq.heappop(notify_queue)[1]
                        yield self._handle_notification(notification)
                    except Exception:
                        logger.exception('error handling notification [{0}]'.format(notification))

        except defer.CancelledError:
            pass

        finally:
            self.listener.unlisten(notification_queue)

    def _handle_notification(self, notification):
        channel, payload = notification
        handler = self.get_handler(channel)
        if not handler:
            logger.warn('no handler for event [{0}]'.format(channel))
            return

        return handler(payload)

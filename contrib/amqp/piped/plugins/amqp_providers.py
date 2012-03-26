import logging

import pika

from piped_amqp import version


logger = logging.getLogger(__name__)


try:
    from pika.adapters import twisted_connection
except ImportError:
    logger.warn('pika {0} does not contain pika.adapters.twisted_connection. piped_amqp will be unavailable'.format(pika.__version__))
    logger.warn('Consider upgrading pika to a new enough version:')
    logger.warn('      $ pip install --upgrade pika')
    logger.warn('   OR $ pip install --upgrade http://github.com/pika/pika/zipball/master#egg=pika')
else:
    from piped_amqp.providers import AMQPConnectionProvider, AMQPConsumerProvider
    from piped_amqp.rpc import RPCClientProvider
from piped_amqp import version
from piped import log

import pika

try:
    from pika.adapters import twisted_connection
except ImportError:
    log.warn('pika {0} does not contain pika.adapters.twisted_connection. piped_amqp will be unavailable'.format(pika.__version__))
    log.warn('Consider upgrading pika to a new enough version:')
    log.warn('      $ pip install --upgrade pika')
    log.warn('   OR $ pip install --upgrade http://github.com/pika/pika/zipball/master#egg=pika')
else:
    from piped_amqp.providers import AMQPConnectionProvider, AMQPConsumerProvider
    from piped_amqp.rpc import RPCClientProvider
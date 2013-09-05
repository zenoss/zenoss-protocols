##############################################################################
#
# Copyright (C) Zenoss, Inc. 2010, all rights reserved.
#
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
#
##############################################################################
import zlib
import errno
import logging
import socket
from amqplib.client_0_8.connection import Connection as amqpConnection
from amqplib.client_0_8.basic_message import Message
from zenoss.protocols.interfaces import IAMQPChannelAdapter
from zenoss.protocols.exceptions import (
        PublishException, NoRouteException, NoConsumersException,
        ConnectionError
    )
from zope.component import getAdapter
from .exceptions import ChannelClosedError


DELIVERY_NONPERSISTENT = 1
DELIVERY_PERSISTENT = 2

REPLY_CODE_NO_ROUTE = 312
REPLY_CODE_NO_CONSUMERS = 313

log = logging.getLogger('zen.%s' % __name__)


def set_keepalive(sock, timeout):
    if timeout > 0:
        # set keepalive on this connection
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        if hasattr(socket, 'TCP_KEEPIDLE'):
            sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPIDLE, timeout)

            interval = max(timeout / 4, 10)
            sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPINTVL, interval)
            sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPCNT, 2)


class Connection(amqpConnection):
    def __init__(self, *args, **kwargs):
        super(Connection, self).__init__(*args, **kwargs)
        if 'keepalive' in kwargs:
            sock = self.connection.transport.sock
            set_keepalive(sock, kwargs['keepalive'])


class Publisher(object):
    """
    A "blocking" way to publish Protobuf messages to an exchange.

    Example:

    with Publisher(amqpConnectionInfo, queueSchema) as publish:
        publish(exchange, routing_key, obj)

    """

    def __init__(self, amqpConnectionInfo, queueSchema):
        self._connection = None
        self._channel = None
        self._connectionInfo = amqpConnectionInfo
        self._schema = queueSchema
        self._exchanges = {}
        self._queues = set()

    def __enter__(self):
        self.getChannel()
        return self.publish

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def getChannel(self):
        """
        Lazily initialize the connection
        """
        try:
            if not self._connection:
                self._connection = Connection(host='%s:%d' % (self._connectionInfo.host, self._connectionInfo.port),
                                              userid=self._connectionInfo.user,
                                              password=self._connectionInfo.password,
                                              virtual_host=self._connectionInfo.vhost,
                                              keepalive=self._connectionInfo.amqpconnectionheartbeat,
                                              ssl=self._connectionInfo.usessl)
                log.debug("Connecting to RabbitMQ...")
            if not self._channel:
                self._channel = self._connection.connection.channel()
        except socket.error as exc:
            # A connection refusal usually means RabbitMQ is down or
            # otherwise inaccessable, so convert exception to
            # ConnectionError and raise it.
            if exc.errno == errno.ECONNREFUSED:
                raise ConnectionError("Could not connect to RabbitMQ", exc)
            raise
        return self._channel

    def useExchange(self, exchange):
        """
        Use an exchange, making sure we only declare it once per connection.
        """
        if exchange not in self._exchanges:
            exchangeConfig = self._schema.getExchange(exchange)
            self._exchanges[exchange] = exchangeConfig
            try:
                channel = self.getChannel()
                getAdapter(channel, IAMQPChannelAdapter).declareExchange(exchangeConfig)
            except Exception as e:
                log.critical("Could not use exchange %s: %s", exchange, e)
                raise
        return self._exchanges[exchange]

    def close(self):
        try:
            if self._channel:
                self._channel.close()
            if self._connection:
                self._connection.close()
        except Exception as e:
            log.info("Error closing RabbitMQ connection: %s", e)
        finally:
            self._reset()

    def _reset(self):
        self._channel = None
        self._connection = None
        self._exchanges = {}
        self._queues = set()

    def publish(self, exchange, routing_key, obj, headers=None, mandatory=False,
                 declareExchange=True):
        """
        Blocking method for publishing items to the queue

        @param obj: The message obj. This obj is expected to be a protobuf
        object and will be serialized to string before being sent.
        @type obj: <protobuf>
        @param exchange: The exchange to publish to.
        @type exchange: str
        @param routing_key: The routing key with which to publish `msg`.
        @type routing_key: str
        """
        # We don't use declareExchange - we already have a caching
        # mechanism to prevent declaring an exchange with each call.
        exchangeConfig = self.useExchange(exchange)

        msg = self.buildMessage(obj, headers, delivery_mode=exchangeConfig.delivery_mode,
                               compression=exchangeConfig.compression)

        lastexc = None
        for i in range(2):
            try:
                channel = self.getChannel()
                log.debug('Publishing with routing key %s to exchange %s', routing_key, exchangeConfig.name)
                channel.basic_publish(msg, exchangeConfig.name, routing_key, mandatory=mandatory)
                if mandatory:
                    self._channel.close()
                    self._channel = None
                    if not channel.returned_messages.empty():
                        reply_code, reply_text, exchange, routing_key, unused = channel.returned_messages.get()
                        if reply_code == REPLY_CODE_NO_ROUTE:
                            raise NoRouteException(reply_code, reply_text, exchange, routing_key)
                        if reply_code == REPLY_CODE_NO_CONSUMERS:
                            raise NoConsumersException(reply_code, reply_text, exchange, routing_key)
                        raise PublishException(reply_code, reply_text, exchange, routing_key)
                break
            except socket.error as exc:
                log.info("RabbitMQ connection was closed: %s", exc)
                lastexc = exc
                self._reset()
        else:
            raise Exception("Could not publish message to RabbitMQ: %s" % lastexc)

    def createQueue(self, queueIdentifier, replacements=None):
        if queueIdentifier not in self._queues:
            queue = self._schema.getQueue(queueIdentifier, replacements)
            lastexc = None
            for i in range(2):
                try:
                    channel = self.getChannel()
                    try:
                        getAdapter(channel, IAMQPChannelAdapter).declareQueue(queue)
                    except ChannelClosedError as e:
                        # Here we handle the case where we redeclare a queue
                        # with different properties. When this happens, Rabbit
                        # both returns an error and closes the channel. We
                        # need to detect this and reopen the channel, since
                        # the existing queue will work fine (although it will
                        # not use the modified config).
                        if e.replyCode == 406:
                            # PRECONDITION_FAILED -- properties changed
                            # Remove the channel and allow it to be reopened
                            log.warn(("Attempted to redeclare queue {0} with "
                                    "different arguments. You will need to "
                                    "delete the queue to pick up the new "
                                    "configuration.").format(queue.name))
                            log.debug(e)
                            self._channel = None
                        else:
                            raise
                    self._queues.add(queueIdentifier)
                    break
                except socket.error as exc:
                    lastexc = exc
                    self._reset()
                except Exception as e:
                    log.exception(e)
                    raise
            else:
                raise Exception("Could not create queue on RabbitMQ: %s" % lastexc)

    def buildMessage(self, obj, headers=None, delivery_mode=DELIVERY_PERSISTENT,
                     compression='none'):

        body = obj.SerializeToString()

        msg_headers = {
            'X-Protobuf-FullName' : obj.DESCRIPTOR.full_name
        }

        msg_properties = {}

        if compression == 'deflate':
            body = zlib.compress(body)
            msg_properties['content_encoding'] = 'deflate'

        if headers:
            msg_headers.update(headers)

        return Message(
            body=body,
            content_type='application/x-protobuf',
            application_headers=msg_headers,
            delivery_mode=delivery_mode,
            **msg_properties)

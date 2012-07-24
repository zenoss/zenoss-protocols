##############################################################################
# 
# Copyright (C) Zenoss, Inc. 2010, all rights reserved.
# 
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
# 
##############################################################################


import logging
from amqplib.client_0_8.connection import Connection
from amqplib.client_0_8.basic_message import Message
from zenoss.protocols.interfaces import IAMQPChannelAdapter
from zenoss.protocols.exceptions import PublishException, NoRouteException, NoConsumersException
from zope.component import getAdapter
import socket

REPLY_CODE_NO_ROUTE = 312
REPLY_CODE_NO_CONSUMERS = 313

log = logging.getLogger('zen.%s' % __name__)

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
        if not self._connection:
            self._connection = Connection(host='%s:%d' % (self._connectionInfo.host, self._connectionInfo.port),
                                          userid=self._connectionInfo.user,
                                          password=self._connectionInfo.password,
                                          virtual_host=self._connectionInfo.vhost,
                                          ssl=self._connectionInfo.usessl)
            log.debug("Connecting to RabbitMQ...")
        if not self._channel:
            self._channel = self._connection.connection.channel()

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
                log.exception(e)
                raise

        return self._exchanges[exchange]

    def close(self):
        try:
            if self._channel:
                self._channel.close()

            if self._connection:
                self._connection.close()
        except Exception as e:
            log.info("error closing publisher %s" % e)
        finally:
            self._reset()

    def _reset(self):
        self._channel = None
        self._connection = None
        self._exchanges = {}
        self._queues = set()

    def publish(self, exchange, routing_key, obj, headers=None, mandatory=False,
                immediate=False, declareExchange=True):
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
        msg = self.buildMessage(obj, headers)

        for i in range(2):
            try:
                channel = self.getChannel()
                # We don't use declareExchange - we already have a caching
                # mechanism to prevent declaring an exchange with each call.
                exchangeConfig = self.useExchange(exchange)
                log.debug('Publishing with routing key %s to exchange %s' % (routing_key, exchangeConfig.name))
                channel.basic_publish(msg, exchangeConfig.name, routing_key, mandatory=mandatory, immediate=immediate)
                if mandatory or immediate:
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
            except socket.error as e:
                log.info("amqp connection was closed %s" % e)
                self._reset()
        else:
            raise Exception("Could not publish message. Connection may be down")

    def createQueue(self, queueIdentifier, replacements=None):
        if queueIdentifier not in self._queues:
            queue = self._schema.getQueue(queueIdentifier, replacements)
            for i in range(2):
                try:
                    channel = self.getChannel()
                    getAdapter(channel, IAMQPChannelAdapter).declareQueue(queue)
                    self._queues.add(queueIdentifier)
                    break
                except socket.error as e:
                    log.info("AMQP connection was closed %s" % e)
                    self._reset()
                except Exception as e:
                    log.exception(e)
                    raise
            else:
                raise Exception("Could not create queue. Connection may be down")

    def buildMessage(self, obj, headers=None):
        msg_headers = {
            'X-Protobuf-FullName' : obj.DESCRIPTOR.full_name
        }

        if headers:
            msg_headers.update(headers)

        return Message(
            body=obj.SerializeToString(),
            content_type='application/x-protobuf',
            application_headers=msg_headers,
            delivery_mode=2 # Persist
        )

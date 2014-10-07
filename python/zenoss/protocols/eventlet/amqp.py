##############################################################################
#
# Copyright (C) Zenoss, Inc. 2010, all rights reserved.
#
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
#
##############################################################################
import zlib
import logging
from zope.component import getAdapter
from zenoss.protocols import hydrateQueueMessage
from zenoss.protocols.interfaces import IAMQPChannelAdapter
from zenoss.protocols.queueschema import SchemaException
from zenoss.protocols.exceptions import ChannelClosedError

from eventlet import patcher
from eventlet.green import socket
amqp = patcher.import_patched('amqplib.client_0_8')
import eventlet
from ..amqp import set_keepalive


DELIVERY_NONPERSISTENT = 1
DELIVERY_PERSISTENT = 2

# basic_qos parameters
UNLIMITED_MESSAGE_SIZE = 0
MESSAGES_PER_WORKER = 1
GLOBAL_QOS = False


__doc__ = """
An eventlet based AMQP publisher/subscriber (consumer).
"""

log = logging.getLogger("zenoss.protocols.eventlet.amqp")

def register_eventlet():
    """
    Registers eventlet to patch the standard socket and time modules. This
    should be called during startup of any daemon using eventlet.
    """
    eventlet.monkey_patch(socket=True, time=True)


class Connection(amqp.Connection):
    def __init__(self, *args, **kwargs):
        super(Connection, self).__init__(*args, **kwargs)
        if 'keepalive' in kwargs:
            sock = self.connection.transport.sock.fd
            set_keepalive(sock, kwargs['keepalive'])


class Publishable(object):
    def __init__(self, message, exchange, routingKey, mandatory=False):
        self.message = message
        self.exchange = exchange
        self.routingKey = routingKey
        self.mandatory = mandatory


class PubSub(object):
    def __init__(self, connection, queueSchema, queueName):
        # Resolve the real queue name and not the identifier
        if queueName:
            queueName = queueSchema.getQueue(queueName).name
        self._connection = connection
        self._channel = None
        self._queueSchema = queueSchema
        self._queueName = queueName
        self._run = False
        self._exchanges = set()
        self._messages_per_worker = MESSAGES_PER_WORKER

    def registerExchange(self, exchange):
        self._exchanges.add(exchange)

    def _onMessage(self, message):
        message.ack = lambda: self.channel.basic_ack(message.delivery_tag)
        message.reject = lambda requeue=True: self.channel.basic_reject(message.delivery_tag, requeue)

        if message.properties.get('content_encoding', None) == 'deflate':
            message.body = zlib.decompress(message.body)

        for publishable in self._processMessage(message):
            self.publish(publishable)

    def _processMessage(self, message):
        raise NotImplementedError()

    def _bind(self):
        queueConfig = self._queueSchema.getQueue(self._queueName)

        try:
            getAdapter(self.channel, IAMQPChannelAdapter).declareQueue(queueConfig)
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
                        "configuration.").format(queueConfig.name))
                log.debug(e)
                self._channel = None
            else:
                raise

        for outboundExchange in self._exchanges:
            exchangeConfig = self._queueSchema.getExchange(outboundExchange)
            getAdapter(self.channel, IAMQPChannelAdapter).declareExchange(exchangeConfig)

    @property
    def channel(self):
        if not self._channel:
            self._channel = self._connection.channel()

        return self._channel

    def _startup(self):
        self._bind()
        self.channel.basic_qos(prefetch_size=UNLIMITED_MESSAGE_SIZE,
                               prefetch_count=self._messages_per_worker,
                               a_global=GLOBAL_QOS)
        self.channel.basic_consume(self._queueName, callback=self._onMessage)

    def run(self):
        self._startup()
        self._run = True

        ch = self.channel
        while self._run and ch.callbacks:
            ch.wait()

    def pop(self):
        """
        Process just one message.
        """
        self._startup()
        if self.channel.callbacks:
            self.channel.wait()

    def publish(self, publishable):
        self.channel.basic_publish(publishable.message, publishable.exchange, publishable.routingKey, mandatory=publishable.mandatory)

    def shutdown(self):
        self._run = False

        if self._connection:
            self._connection.close()
            self._connection = None

    @property
    def messagesPerWorker(self):
        return self._messages_per_worker

    @messagesPerWorker.setter
    def messagesPerWorker(self, value):
        self._messages_per_worker = value


class ProtobufPubSub(PubSub):
    def __init__(self, connection, queueSchema, queueName):
        super(ProtobufPubSub, self).__init__(connection, queueSchema, queueName)
        self._handlers = {}

    def registerHandler(self, contentType, handler):
        fullName = self._queueSchema.getContentType(contentType).protobuf_name
        self._handlers[fullName] = handler

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

        return amqp.Message(
            body=body,
            content_type='application/x-protobuf',
            application_headers=msg_headers,
            delivery_mode=delivery_mode,
            **msg_properties)


    def publish(self, publishable):

        exchangeConfig = self._queueSchema.getExchange(publishable.exchange)
        publishable.message = self.buildMessage(publishable.message,
                                               delivery_mode=exchangeConfig.delivery_mode,
                                               compression=exchangeConfig.compression)
        publishable.exchange = exchangeConfig.name

        return super(ProtobufPubSub, self).publish(publishable)

    def _processMessage(self, message):
        try:
            proto = hydrateQueueMessage(message, self._queueSchema)

            try:
                handler = self._handlers[proto.DESCRIPTOR.full_name]
            except KeyError:
                raise Exception('No message handler for "%s"' % proto.DESCRIPTOR.full_name)

            for publishable in handler(message, proto):
                yield publishable

        except SchemaException:
            # received an invalid message log it and move on
            log.error("Unable to hydrate protobuf %s with headers %s " % (message.body, message.properties.get('application_headers')))

            # we can't process the message so throw it away
            message.ack()


def getProtobufPubSub(amqpConnectionInfo, queueSchema, queue, connection=None):
    if connection is None:
        connection = Connection(
            host = '%s:%d' % (amqpConnectionInfo.host, amqpConnectionInfo.port),
            userid = amqpConnectionInfo.user,
            password = amqpConnectionInfo.password,
            ssl = amqpConnectionInfo.usessl,
            virtual_host = amqpConnectionInfo.vhost,
            keepalive = amqpConnectionInfo.amqpconnectionheartbeat,
        )
    pubsub = ProtobufPubSub(connection, queueSchema, queue)
    return pubsub

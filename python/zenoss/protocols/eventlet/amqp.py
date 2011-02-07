###########################################################################
#
# This program is part of Zenoss Core, an open source monitoring platform.
# Copyright (C) 2010, Zenoss Inc.
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License version 2 as published by
# the Free Software Foundation.
#
# For complete information please visit: http://www.zenoss.com/oss/
#
###########################################################################

from zenoss.protocols import queueschema, hydrateQueueMessage
from zenoss.protocols.amqpconfig import getAMQPConfiguration

from eventlet import patcher
from eventlet.green import socket
amqp = patcher.import_patched('amqplib.client_0_8', socket=socket)
import eventlet
eventlet.monkey_patch(socket=True, time=True)

__doc__ = """
An eventlet based AMQP publisher/subscriber (consumer).
"""

class Connection(amqp.Connection):
    pass

class Publishable(object):
    def __init__(self, message, exchange, routingKey, mandatory=False):
        self.message = message
        self.exchange = exchange
        self.routingKey = routingKey
        self.mandatory = mandatory

class PubSub(object):
    def __init__(self, connection, queueName):
        self._connection = connection
        self._channel = None
        self._queueName = queueName
        self._run = False
        self._bindings = {}
        self._exchanges = set()

    def registerExchange(self, exchange):
        self._exchanges.add(exchange)

    def _onMessage(self, message):
        message.ack = lambda: self.channel.basic_ack(message.delivery_tag)
        message.reject = lambda requeue=True: self.channel.basic_reject(message.delivery_tag, requeue)
        for publishable in self._processMessage(message):
            self.publish(publishable)

    def _processMessage(self, message):
        raise NotImplementedError()

    def bind(self, exchange, routingKeys):
        if not isinstance(routingKeys, list):
            routingKeys = list(routingKeys)

        if exchange not in self._bindings:
            self._bindings[exchange] = set([])

        self._bindings[exchange].update(routingKeys)

    def _bind(self):
        config = getAMQPConfiguration()

        for outboundExchange in self._exchanges:
            config.declareExchange(self.channel, outboundExchange)

        for inboundExchange, routingKeys in self._bindings.iteritems():
            config.declareExchange(self.channel, inboundExchange)
            config.declareQueue(self.channel, self._queueName)

    @property
    def channel(self):
        if not self._channel:
            self._channel = self._connection.channel()

        return self._channel

    def _startup(self):
        self._bind()

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

class ProtobufPubSub(PubSub):
    def __init__(self, connection, queueName):
        queue = None
        if queueName:
            queue = queueschema.getQueue(queueName)
            queueName = queue.name

        super(ProtobufPubSub, self).__init__(connection, queueName)

        if queue:
            for binding in queue.bindings.values():
                self.bind(binding.exchange.name, binding.routing_key)

        self._handlers = {}

    def registerHandler(self, contentType, handler):
        fullName = queueschema.getContentType(contentType).protobuf_name
        self._handlers[fullName] = handler

    def buildMessage(self, obj, headers=None):
        msg_headers = {
            'X-Protobuf-FullName' : obj.DESCRIPTOR.full_name
        }

        if headers:
            msg_headers.update(headers)

        return amqp.Message(
            body=obj.SerializeToString(),
            content_type='application/x-protobuf',
            application_headers=msg_headers
        )

    def publish(self, publishable):

        publishable.message = self.buildMessage(publishable.message)
        publishable.exchange = queueschema.getExchange(publishable.exchange).name

        return super(ProtobufPubSub, self).publish(publishable)

    def _processMessage(self, message):
        proto = hydrateQueueMessage(message)

        try:
            handler = self._handlers[proto.DESCRIPTOR.full_name]
        except KeyError:
            raise Exception('No message handler for "%s"' % proto.DESCRIPTOR.full_name)

        for publishable in handler(message, proto):
            yield publishable


def getProtobufPubSub(queue, connection=None):
    if connection is None:
        config = getAMQPConfiguration()
        connection = Connection(
            host = '%s:%d' % (config.host, config.port),
            userid = config.user,
            password = config.password,
            ssl = config.usessl,
            virtual_host = config.vhost
        )
    pubsub = ProtobufPubSub(connection, queue)
    return pubsub


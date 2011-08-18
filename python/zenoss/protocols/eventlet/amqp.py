###########################################################################
#
# This program is part of Zenoss Core, an open source monitoring platform.
# Copyright (C) 2010, Zenoss Inc.
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License version 2 or (at your
# option) any later version as published by the Free Software Foundation.
#
# For complete information please visit: http://www.zenoss.com/oss/
#
###########################################################################

import logging
from zope.component import getAdapter
from zenoss.protocols import hydrateQueueMessage
from zenoss.protocols.interfaces import IAMQPChannelAdapter
from zenoss.protocols.queueschema import SchemaException

from eventlet import patcher
from eventlet.green import socket
amqp = patcher.import_patched('amqplib.client_0_8', socket=socket)
import eventlet
eventlet.monkey_patch(socket=True, time=True)

__doc__ = """
An eventlet based AMQP publisher/subscriber (consumer).
"""

log = logging.getLogger("zenoss.protocols.eventlet.amqp")

class Connection(amqp.Connection):
    pass


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

    def registerExchange(self, exchange):
        self._exchanges.add(exchange)

    def _onMessage(self, message):
        message.ack = lambda: self.channel.basic_ack(message.delivery_tag)
        message.reject = lambda requeue=True: self.channel.basic_reject(message.delivery_tag, requeue)
        for publishable in self._processMessage(message):
            self.publish(publishable)

    def _processMessage(self, message):
        raise NotImplementedError()

    def _bind(self):
        queueConfig = self._queueSchema.getQueue(self._queueName)
        getAdapter(self.channel, IAMQPChannelAdapter).declareQueue(queueConfig)

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
    def __init__(self, connection, queueSchema, queueName):
        super(ProtobufPubSub, self).__init__(connection, queueSchema, queueName)
        self._handlers = {}

    def registerHandler(self, contentType, handler):
        fullName = self._queueSchema.getContentType(contentType).protobuf_name
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
        publishable.exchange = self._queueSchema.getExchange(publishable.exchange).name

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
            virtual_host = amqpConnectionInfo.vhost
        )
    pubsub = ProtobufPubSub(connection, queueSchema, queue)
    return pubsub


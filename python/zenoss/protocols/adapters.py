###########################################################################
#
# This program is part of Zenoss Core, an open source monitoring platform.
# Copyright (C) 2011, Zenoss Inc.
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License version 2 or (at your
# option) any later version as published by the Free Software Foundation.
#
# For complete information please visit: http://www.zenoss.com/oss/
#
###########################################################################

from __future__ import absolute_import
import pkg_resources
import logging
from twisted.internet.defer import returnValue, inlineCallbacks
from amqplib.client_0_8.channel import Channel as AMQPLibChannel
from txamqp.protocol import AMQChannel as TwistedAMQChannel
from zenoss.protocols.interfaces import IAMQPChannelAdapter
from zope.interface import implements
from zope.component import adapts

log = logging.getLogger('zen.protocols')

class AMQPLibChannelAdapter(object):
    
    implements(IAMQPChannelAdapter)
    adapts(AMQPLibChannel)

    def __init__(self, channel):
        self.channel = channel

    def declareQueue(self, queue):
        log.debug("Creating queue: %s", queue.name)
        result = self.channel.queue_declare(queue=queue.name,
                                            durable=queue.durable,
                                            exclusive=queue.exclusive,
                                            auto_delete=queue.auto_delete,
                                            arguments=queue.arguments)

        for identifier, binding in queue.bindings.iteritems():
            self.declareExchange(binding.exchange)

            log.debug("Binding queue %s to exchange %s with routing_key %s",
                queue.name, binding.exchange.name, binding.routing_key)
            self.channel.queue_bind(queue=queue.name,
                                    exchange=binding.exchange.name,
                                    routing_key=binding.routing_key,
                                    arguments=binding.arguments)
        return result


    def declareExchange(self, exchange):
        log.debug("Creating exchange: %s", exchange.name)
        return self.channel.exchange_declare(exchange.name, exchange.type,
                                             durable=exchange.durable,
                                             auto_delete=exchange.auto_delete,
                                             arguments=exchange.arguments)

class TwistedChannelAdapter(object):

    implements(IAMQPChannelAdapter)
    adapts(TwistedAMQChannel)

    def __init__(self, channel):
        self.channel = channel

    @inlineCallbacks
    def declareQueue(self, queue):
        log.debug("Creating queue: %s", queue.name)
        result = yield self.channel.queue_declare(queue=queue.name,
                                                  durable=queue.durable,
                                                  exclusive=queue.exclusive,
                                                  auto_delete=queue.auto_delete,
                                                  arguments=queue.arguments)

        for identifier, binding in queue.bindings.iteritems():
            yield self.declareExchange(binding.exchange)

            log.debug("Binding queue %s to exchange %s with routing_key %s", queue.name, binding.exchange.name,
                      binding.routing_key)
            yield self.channel.queue_bind(queue=queue.name,
                                      exchange=binding.exchange.name,
                                      routing_key=binding.routing_key,
                                      arguments=binding.arguments)
        returnValue(result)

    @inlineCallbacks
    def declareExchange(self, exchange):
        log.debug("Creating exchange: %s", exchange.name)
        # AMQP 0.9.1 (which txAMQP uses) has deprecated auto-delete on exchanges
        result = yield self.channel.exchange_declare(exchange=exchange.name,
                                                     type=exchange.type,
                                                     durable=exchange.durable,
                                                     arguments=exchange.arguments)
        returnValue(result)

def registerAdapters():
    from zope.component import provideAdapter
    provideAdapter(AMQPLibChannelAdapter)
    provideAdapter(TwistedChannelAdapter)

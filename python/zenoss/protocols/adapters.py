##############################################################################
# 
# Copyright (C) Zenoss, Inc. 2011, all rights reserved.
# 
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
# 
##############################################################################


from __future__ import absolute_import
import pkg_resources
import logging
from twisted.internet.defer import returnValue, inlineCallbacks
from amqplib.client_0_8.channel import Channel as AMQPLibChannel
from amqplib.client_0_8.connection import Connection as AMQPConnection
from amqplib.client_0_8.exceptions import AMQPChannelException
from txamqp.protocol import AMQChannel as TwistedAMQChannel
from txamqp.client import Closed
from zenoss.protocols.interfaces import IAMQPChannelAdapter
from zope.interface import implements
from zope.component import adapts
from .exceptions import ChannelClosedError

log = logging.getLogger('zen.protocols')

class AMQPLibChannelAdapter(object):
    
    implements(IAMQPChannelAdapter)
    adapts(AMQPLibChannel)

    def __init__(self, channel):
        self.channel = channel

    def declareQueue(self, queue):
        log.debug("Creating queue: %s", queue.name)
        log.debug("Using arguments: %r", queue.arguments)
        try:
            result = self.channel.queue_declare(queue=queue.name,
                                                durable=queue.durable,
                                                exclusive=queue.exclusive,
                                                auto_delete=queue.auto_delete,
                                                arguments=queue.arguments)
        except AMQPChannelException as e:
            raise ChannelClosedError(e)

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

    def deleteQueue(self, queue):
        try:
            log.debug("Deleting queue: %s", queue.name)
            self.channel.queue_purge(queue.name)

            log.debug('Deleting queue: %s', queue.name)
            self.channel.queue_delete(queue.name)
        except AMQPChannelException, e:
            # if the queue doesn't exist, don't worry about it.
            if e.amqp_reply_code == 404:
                log.debug('Queue %s did not exist', queue.name)
            else:
                raise

class TwistedChannelAdapter(object):

    implements(IAMQPChannelAdapter)
    adapts(TwistedAMQChannel)

    def __init__(self, channel):
        self.channel = channel

    @inlineCallbacks
    def declareQueue(self, queue):
        log.debug("Creating queue: %s", queue.name)
        log.debug("Using arguments: %r", queue.arguments)
        try:
            result = yield self.channel.queue_declare(queue=queue.name,
                                                      durable=queue.durable,
                                                      exclusive=queue.exclusive,
                                                      auto_delete=queue.auto_delete,
                                                      arguments=queue.arguments)
        except Closed as e:
            raise ChannelClosedError(e)

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

    def deleteQueue(self, queue):
        raise NotImplementedError("deleteQueue not implemented")

class AMQPUtil(object):

    def getConnection(self, amqpConnectionInfo):
        conn = AMQPConnection(host="%s:%s" % (amqpConnectionInfo.host, amqpConnectionInfo.port),
                              userid=amqpConnectionInfo.user,
                              password=amqpConnectionInfo.password,
                              virtual_host=amqpConnectionInfo.vhost,
                              ssl=amqpConnectionInfo.usessl)
        return conn

    def getChannel(self, connectionInfo=None, connection=None):
        """
        Return a tuple of (channel, connection)
        """
        if connection:
            amqpConnection = connection
        elif connectionInfo:
            amqpConnection = self.getConnection(connectionInfo)
        else:
            raise Exception("connectionInfo or connection must be passed in")
        channel = amqpConnection.channel()
        return (channel, amqpConnection)

#TODO create a util for creating txamqp factories and channels
#class TXAMQPUtil(object):
#
#    @inlineCallbacks
#    def createFactory(self, connectionInfo, queueSchema):
#        factory = AMQPFactory(connectionInfo, queueSchema)
#        yield factory._onConnectionMade
#        returnValue(factory)
#
#    @inlineCallbacks
#    def getChannel(self, connectionInfo, queueSchema):
#        factory = yield self.connectFactory(connectionInfo, queueSchema)
#        channel = yield factory.get_channel()
#        returnValue(channel)



def registerAdapters():
    from zope.component import provideAdapter
    provideAdapter(AMQPLibChannelAdapter)
    provideAdapter(TwistedChannelAdapter)

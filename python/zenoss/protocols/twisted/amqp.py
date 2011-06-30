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
from os.path import join as pathjoin, dirname
from twisted.internet import reactor
from twisted.internet.protocol import ReconnectingClientFactory
from twisted.internet import defer
from twisted.internet.defer import inlineCallbacks, returnValue, Deferred, DeferredList
from txamqp import spec
from txamqp.queue import Closed
from txamqp.protocol import AMQClient
from txamqp.client import TwistedDelegate
from txamqp.content import Content
from zenoss.protocols.amqpconfig import getTwistedAMQPConfiguration


log = logging.getLogger('zen.protocols.twisted')


class PersistentMessage(Content):
    """
    Simple wrapper for C{Content} that sets delivery_mode to persistent.
    """
    def __init__(self, *args, **kwargs):
        Content.__init__(self, *args, **kwargs)
        self['delivery_mode'] = 2


class AMQProtocol(AMQClient):
    """
    Protocol for an AMQ connection. Has methods for sending to an exchange and
    listening to a queue.

    Protocols get created every time a connection is made, so we can't store
    anything on here that needs to outlive a given connection; we'll store
    things on the factory and ask it for that information.
    """
    _connected = False

    @inlineCallbacks
    def connectionMade(self):
        """
        Hook called when the connection is made; we'll use this to perform
        exchange setup, etc.
        """
        config = getTwistedAMQPConfiguration()
        AMQClient.connectionMade(self)
        log.debug('Made initial connection to message broker')
        self._connected = False
        # Authenticate
        yield self.start({'LOGIN':config.user, 'PASSWORD':config.password})
        log.debug('Successfully authenticated as %s' % config.user)
        # Get a channel
        self.chan = yield self.get_channel()
        self._connected = True
        # Initialize the queues
        yield self.begin_listening()
        # Call back our deferred
        self.factory.onConnectionMade(self)
        # Flush any messages that have been sent before now
        yield self.send()
        returnValue(None)

    def is_connected(self):
        return self._connected

    @inlineCallbacks
    def get_channel(self):
        """
        Get a channel.
        """
        chan = yield self.channel(2)
        yield chan.channel_open()
        log.debug('Channel opened')
        returnValue(chan)

    @inlineCallbacks
    def listen_to_queue(self, exchange, exchange_type, routing_key, queue_name, callback):
        """
        Get a queue and register a callback to be executed when a message is
        received, then begin listening.
        """
        if self.is_connected():
            queue = yield self.get_queue(exchange, exchange_type, routing_key, queue_name)
            log.debug('Listening to queue %s.%s' % (exchange, routing_key))
            # Start the recursive call to listen for messages
            yield self.processMessages(queue, callback)

    @inlineCallbacks
    def begin_listening(self):
        """
        Iterate over all queues registered in the factory and start listening
        to them.
        """
        log.debug('Binding to %s queues' % len(self.factory.queues))
        for exchange, exchange_type, routing_key, queue_name, cb in self.factory.queues:
            yield self.listen_to_queue(exchange, exchange_type, routing_key, queue_name, cb)

    @inlineCallbacks
    def get_queue(self, exchange, exchange_type, routing_key, queue_name):
        """
        Perform all the setup to get a queue, then return it.
        """
        yield self.create_queue(exchange, queue_name)

        # Start consuming from the queue (this actually creates it)
        yield self.chan.basic_consume(queue=queue_name,
                                      consumer_tag=queue_name)
        # Go get the queue and return it
        queue = yield self.queue(queue_name)
        returnValue(queue)

    @inlineCallbacks
    def create_queue(self, exchangeIdentifier, queueIdentifier):
        # Declare the exchange
        config = getTwistedAMQPConfiguration()
        yield config.declareExchange(self.chan, exchangeIdentifier)

        # Declare the queue
        yield config.declareQueue(self.chan, queueIdentifier)

    @inlineCallbacks
    def send_message(self, exchange, routing_key, msg, mandatory=False, immediate=False):
        body = msg
        headers = {}

        # it is a protobuf
        if not isinstance(msg, basestring):
            body = msg.SerializeToString()
            headers = {
                'X-Protobuf-FullName':'{name}'.format(
                    name=msg.DESCRIPTOR.full_name
                    ),
                }
        config = getTwistedAMQPConfiguration()
        # Declare the exchange to which the message is being sent
        yield config.declareExchange(self.chan, exchange)

        # Wrap the message in our Content subclass
        content = PersistentMessage(body)
        # set the headers to our protobuf type, hopefully this works
        content.properties['headers'] = headers
        content.properties['content-type'] = 'application/x-protobuf'

        # Publish away
        exchangeConfig = config.getExchange(exchange)
        yield self.chan.basic_publish(exchange=exchangeConfig.name,
                                      routing_key=routing_key,
                                      content=content,
                                      mandatory=mandatory,
                                      immediate=immediate)
        returnValue("SUCCESS")

    def send(self):
        """
        Send any messages queued on the factory.
        """
        if self.is_connected():
            dList = []
            while self.factory.messages:
                message_args = self.factory.messages.pop()
                dList.append(self.send_message(*message_args))
            d = DeferredList(dList)
            d.addCallback(self.factory.onInitialSend)
            return d
        return self.factory._onInitialSend

    def acknowledge(self, message):
        """
        Acknowledges a message
        """
        self.chan.basic_ack(delivery_tag=message.delivery_tag, multiple=False)

    @inlineCallbacks
    def processMessages(self, queue, callback):
        """
        Gets messages from a queue and fires callbacks, then calls itself to
        continue listening.
        """
        try:
            message = yield queue.get()
        except Closed:
            log.debug('Connection to queue closed')
        else:
            reactor.callLater(0, self._doCallback, queue, callback, message)
            returnValue(None)

    @inlineCallbacks
    def _doCallback(self, queue, callback, message):
        yield defer.maybeDeferred(callback, message)
        self.processMessages(queue, callback)

    def connectionLost(self, reason):
        log.debug("connection lost %s" % reason)
        AMQClient.connectionLost(self, reason)
        self.factory.onConnectionLost(reason)


class AMQPFactory(ReconnectingClientFactory):
    """
    The actual service. This is what should be used to listen to queues and
    send messages; these are buffered on the factory, then referenced by the
    protocol each time a connection is made.
    """
    protocol = AMQProtocol

    def __init__(self):
        with open(pathjoin(dirname(__file__), 'amqp0-9-1.xml')) as f:
            self.spec = spec.load(f)
            log.debug('Loaded AMQP spec')
        config = getTwistedAMQPConfiguration()
        self.vhost = config.vhost
        self.host = config.host
        self.port = config.port
        self.usessl = config.usessl
        self.delegate = TwistedDelegate()
        self.queues = []
        self.messages = []
        self.p = None
        self._onInitialSend = Deferred()
        self._onConnectionMade = Deferred()
        self._onConnectionLost = Deferred()
        self.connector = reactor.connectTCP(self.host, self.port, self)

    def onConnectionMade(self, value):
        d,self._onConnectionMade = self._onConnectionMade, Deferred()
        d.callback(value)

    def onConnectionLost(self, value):
        d,self._onConnectionLost = self._onConnectionLost, Deferred()
        d.callback(value)

    def onInitialSend(self, value):
        d,self._onInitialSend = self._onInitialSend, Deferred()
        d.callback(value)

    def buildProtocol(self, addr):
        self.p = self.protocol(self.delegate, self.vhost, self.spec)
        self.p.factory = self
        self.resetDelay()
        return self.p

    def listen(self, exchange, exchange_type, routing_key, queue_name, callback):
        """
        Listen to a queue.

        @param exchange: The exchange to send to
        @type exchange: str
        @param exchange_type: The type of the exchange.
        @type exchange_type: str
        @param routing_key: The routing_key for the message
        @type routing_key: str
        @param callback: The function to be called when a message is received
        in this queue.
        @type message: callable
        """
        args = exchange, exchange_type, routing_key, queue_name, callback
        self.queues.append(args)
        if self.p is not None:
            self.p.listen_to_queue(*args)

    def send(self, exchangeIdentifier, routing_key, message, mandatory=False, immediate=False):
        """
        Send a C{message} to exchange C{exchange}.

        Appends to the factory message buffer, then tells the protocol to read
        from that buffer.

        @param exchangeIdentifier: The exchange to send to
        @type exchange: str
        @param routing_key: The routing_key for the message
        @type routing_key: str
        @param message: The message to send
        @type message:str
        """
        self.messages.append((exchangeIdentifier, routing_key, message, mandatory, immediate))
        if self.p is not None:
            return self.p.send()
        else:
            return self._onInitialSend


    def acknowledge(self, message):
        """
        Acknowledges a message so it is removed from the queue
        """
        self.p.acknowledge(message)

    def shutdown(self):
        """
        Disconnect completely and call back our connectionLost Deferred.
        """
        self.connector.disconnect()
        self.stopTrying()
        shutdownDeferred = Deferred()
        def connectionShut(result):
            shutdownDeferred.callback("connection shut down")
        self._onConnectionLost.addBoth(connectionShut)
        return shutdownDeferred

    @property
    def channel(self):
        return self.p.chan

    def createQueue(self, exchange, queueIdentifier):
        def doCreateQueue(value):
            return self.p.create_queue(exchange,  queueIdentifier)
        if self.p is not None:
            return doCreateQueue(None)
        else:
            self._onConnectionMade.addCallback(doCreateQueue)
            return self._onConnectionMade


def test():
    def cb(m):
        print "Received: %r" % m
    amqp = AMQPFactory()
    amqp.listen(exchange='zenoss.dependencies',
                routing_key='graphchanges',
                callback=cb)
    reactor.run()

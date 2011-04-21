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

from __future__ import absolute_import
from collections import defaultdict
import logging
from zenoss.protocols import queueschema
import warnings
from twisted.internet.defer import returnValue, inlineCallbacks


log = logging.getLogger('zen.protocols')

# TODO the queue configuration and the schema are not related and should be separate objects
class AMQPConfig(object):
    """
    Class that encapsulates the configuration for
    all of the various queues that zenoss uses.
    """
    _options = [
        dict(short_opt='H', long_opt='amqphost', type='string', key='host', default='localhost', help='Rabbitmq server host'),
        dict(short_opt='P', long_opt='amqpport', type='int', key='port', default=5672, help='Rabbitmq server port', parser=int),
        dict(short_opt='V', long_opt='amqpvhost', type='string', key='vhost', default='/zenoss', help='Rabbitmq server virtual host'),
        dict(short_opt='u', long_opt='amqpuser', type='string', key='user', default='zenoss', help='User to connect as'),
        dict(short_opt='p', long_opt='amqppassword', type='string', key='password', default='zenoss', help='Password to connect with'),
        dict(short_opt='s', long_opt='amqpusessl', type='int', key='usessl', default=False, help='User SSL to connect to the server', parser=lambda v: str(v).lower() in ('true', 'y', 'yes', '1')),
    ]

    def __init__(self, **kwargs):
        """
        Initialize with optional settings as keyword arguments.
        
        See AMQPConfig._options for list of valid options.
        """
        self._host = None
        self._port = None
        self._vhost = None
        self._user = None
        self._password = None
        self._usessl = None
        self._optionMap = None

        defaults = dict([(o['key'], o['default']) for o in self._options])
        if kwargs:
            defaults.update(kwargs)

        self.update(defaults)

    def _getOptionMap(self):
        """
        Create a map of options to their storage keys. Used for fast lookup of options.
        """
        if self._optionMap is None:
            self._optionMap = {}
            for option in self._options:
                self._optionMap[option['long_opt']] = option

        return self._optionMap

    def update(self, options):
        """
        Update the settings from a dictionary.

        See AMQPConfig._options for list of valid options.    
        """
        optionMap = self._getOptionMap()

        if isinstance(options, dict):
            options = options.iteritems()
        elif isinstance(options, object):
            # Looks like a plain old object with properties as options
            options = options.__dict__.iteritems()

        for key, value in options:
            if key in optionMap:
                parser = optionMap[key].get('parser', str)
                setattr(self, '_' + optionMap[key]['key'], parser(value))

    def getContentType(self, identifier):
        warnings.warn('Use zenoss.protocols.queueschema.getContentType() instead', DeprecationWarning)
        return queueschema.getContentType(identifier)

    def getExchange(self, identifier):
        warnings.warn('Use zenoss.protocols.queueschema.getExchange() instead', DeprecationWarning)
        return queueschema.getExchange(identifier)

    def getQueue(self, identifier):
        warnings.warn('Use zenoss.protocols.queueschema.getQueue() instead', DeprecationWarning)
        return queueschema.getQueue(identifier)

    def getNewProtobuf(self, identifier):
        warnings.warn('Use zenoss.protocols.queueschema.getNewProtobuf() instead', DeprecationWarning)
        return queueschema.getNewProtobuf(identifier)

    def hydrateProtobuf(self, protobuf_name, content):
        warnings.warn('Use zenoss.protocols.queueschema.hydrateProtobuf() instead', DeprecationWarning)
        return queueschema.hydrateProtobuf(protobuf_name, content)

    def declareQueues(self, channel):
        """
        Iterates over every queue defined and establishes the
        binding and the queue.

        @type  channel: AQMPChannel
        @param channel: The connection to the amqp sever
        """
        for queue in queueschema.getQueues():
            self.declareQueue(channel, queue.identifier)

    def declareExchanges(self, channel):
        """
        Declares every exchange.

        @type  channel: AQMPChannel
        @param channel: The connection to the amqp sever
        """
        for exchange in queueschema.getExchanges():
            self.declareExchange(channel, exchange.identifier)

    def declareQueue(self, channel, queueIdentifier, replacements=None):
        """
        Creates the queue and binds it to the exchange using the schema
        configuration.

        @type  channel: AQMPChannel
        @param channel: The connection to the amqp sever
        @type  queueIdentifier: string
        @param queueIdentifier: The identifier of the queue we wish to create
        @type replacements: dict
        @param replacements: Strings that should replace python string formatting in the routing key
        """
        queueConfig = queueschema.getQueue(queueIdentifier)
        qname = queueConfig.name
        for identifier, binding in queueConfig.bindings.iteritems():
            routing_key = binding.routing_key
            if replacements:
                # Replace args in the routing key with the replacements specified,
                # or '#' if none
                d = defaultdict(lambda:'#')
                d.update(replacements)
                routing_key = routing_key % d
            log.debug("Creating queue %s with routing_key %s to exchange  %s" % (
                qname, routing_key, binding.exchange.name))
            channel.queue_declare(queue=qname, durable=True, auto_delete=False)
            result = channel.queue_bind(queue=qname, exchange=binding.exchange.name,
                                    routing_key=routing_key)
        return result

    def declareExchange(self, channel, exchangeIdentifier):
        """
        Creates the queue and binds it to the exchange using the schema
        configuration.

        @type  channel: AQMPChannel
        @param channel: The connection to the amqp sever
        @type  queueIdentifier: string
        @param queueIdentifier: The identifier of the queue we wish to create
        """
        exchangeConfig = queueschema.getExchange(exchangeIdentifier)
        log.debug("Creating exchange: %s" % exchangeConfig.name)
        return channel.exchange_declare(exchangeConfig.name, type=exchangeConfig.type,
                                           durable=exchangeConfig.durable, auto_delete=exchangeConfig.auto_delete)

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def vhost(self):
        return self._vhost

    @property
    def user(self):
        return self._user

    @property
    def password(self):
        return self._password

    @property
    def usessl(self):
        return self._usessl
        
    @classmethod
    def addOptionsToParser(cls, parser):
        """
        Populate an OptionPaser with our options.
        """
        for option in cls._options:
            parser.add_option(
                '-' + option['short_opt'],
                '--' + option['long_opt'],
                type=option['type'],
                dest=option['long_opt'],
                help=option['help'],
                default=option['default'],
                action='store',
            )

        return parser

class TwistedAMQPConfig(AMQPConfig):
    
    _overridden = ['declareQueues','declareExchanges', 'declareQueue','declareExchange']
    
    def __init__(self, delegateConfig):
        self._delegate = delegateConfig
    
    @inlineCallbacks
    def declareQueues(self, channel):
        """
        Iterates over every queue defined and establishes the
        binding and the queue.
        
        @type  channel: AQMPChannel
        @param channel: The connection to the amqp sever    
        """
        for queue in queueschema.getQueues():
            yield self.declareQueue(channel, queue.identifier)

    @inlineCallbacks
    def declareExchanges(self, channel):
        """
        Declares every exchange.
        
        @type  channel: AQMPChannel
        @param channel: The connection to the amqp sever        
        """
        for exchange in queueschema.getExchanges():
            yield self.declareExchange(channel, exchange.identifier)

    @inlineCallbacks
    def declareQueue(self, channel, queueIdentifier):
        """
        Creates the queue and binds it to the exchange using the schema
        configuration.
        
        @type  channel: AQMPChannel
        @param channel: The connection to the amqp sever
        @type  queueIdentifier: string
        @param queueIdentifier: The identifier of the queue we wish to create
        """
        queueConfig = queueschema.getQueue(queueIdentifier)
        qname = queueConfig.name
        for identifier, binding in queueConfig.bindings.iteritems():
            log.debug("Creating queue %s with routing_key %s to exchange  %s" % (queueConfig.name, binding.routing_key, binding.exchange.name))
            yield channel.queue_declare(queue=qname, durable=True,
                                       auto_delete=False)
            result = yield channel.queue_bind(queue=qname, exchange=binding.exchange.name,
                                    routing_key=binding.routing_key)
        returnValue( result )

    @inlineCallbacks
    def declareExchange(self, channel, exchangeIdentifier):
        """
        Creates the queue and binds it to the exchange using the schema
        configuration.
        
        @type  channel: AQMPChannel
        @param channel: The connection to the amqp sever
        @type  queueIdentifier: string
        @param queueIdentifier: The identifier of the queue we wish to create
        """
        exchangeConfig = queueschema.getExchange(exchangeIdentifier)
        log.debug("Creating exchange: %s" % exchangeConfig.name)
        durable = exchangeConfig.durable
        type = exchangeConfig.type
        name = exchangeConfig.name
        result = yield channel.exchange_declare(exchange=name, type=type, durable=durable )
        returnValue(result)
        
    def __getattribute__(self,name):
        if name in object.__getattribute__(self,'_overridden'):
            return object.__getattribute__(self, name)
        else:
            delegate = object.__getattribute__(self,'_delegate')
            return delegate.__getattribute__(name)


_CONFIGURATION  = AMQPConfig()
_TWISTEDCONFIG = TwistedAMQPConfig(_CONFIGURATION)
def setAMQPConfiguration(config):
    _CONFIGURATION.update(config)
    return _CONFIGURATION

def getAMQPConfiguration():
    return _CONFIGURATION

def getTwistedAMQPConfiguration():
    return _TWISTEDCONFIG


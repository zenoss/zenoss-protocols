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

from zope.dottedname.resolve import resolve
from zenoss.protocols.data import queueschema

__all__ = [
    'ContentType',
    'Binding',
    'Queue',
    'Exchange',
    'Schema',
    'getContentType',
    'getNewProtobuf',
    'hydrateProtobuf',
    'getExchange',
    'getExchanges',
    'getQueue',
    'getQueues',
]

class SchemaException(Exception):
    pass

# create readonly-ish objects that represent the configuration
class ContentType(object):
    def __init__(self, identifier, java_class, python_class, content_type, protobuf_name):
        self.identifier = identifier
        self._java_class = java_class
        self._python_class = python_class
        self._content_type = content_type
        self._protobuf_name = protobuf_name

    @property
    def java_class(self):
        return self._java_class

    @property
    def python_class(self):
        return self._python_class

    @property
    def content_type(self):
        return self._content_type

    @property
    def protobuf_name(self):
        return self._protobuf_name


class Binding(object):
    def __init__(self, exchange, routing_key):
        self._exchange = exchange
        self._routing_key = routing_key

    @property
    def exchange(self):
        return self._exchange

    @property
    def routing_key(self):
        return self._routing_key


class Queue(object):
    def __init__(self, identifier, name, durable, exclusive, auto_delete, description):
        self.identifier = identifier
        self._name = name
        self._durable = durable
        self._exclusive = exclusive
        self._auto_delete = auto_delete
        self._description = description
        self._bindings = {}

    def bind(self, binding):
        self._bindings[binding.exchange.identifier] = binding

    @property
    def name(self):
        return self._name

    @property
    def durable(self):
        return self._durable

    @property
    def exclusive(self):
        return self._exclusive

    @property
    def auto_delete(self):
        return self._auto_delete

    @property
    def description(self):
        return self._description

    @property
    def bindings(self):
        return self._bindings

    def getBinding(self, exchangeIdentifier):
        return self._bindings[exchangeIdentifier]

class Exchange(object):
    def __init__(self, identifier, name, type, durable, auto_delete, description, content_types, routing_key_regexp):
        self.identifier = identifier
        self._name = name
        self._type = type
        self._auto_delete = auto_delete
        self._durable = durable
        self._description = description
        self._content_types = content_types
        self._routing_key_regexp = routing_key_regexp

    @property
    def name(self):
         return self._name

    @property
    def type(self):
        return self._type

    @property
    def durable(self):
        return self._durable

    @property
    def auto_delete(self):
        return self._auto_delete

    @property
    def description(self):
        return self._description

    @property
    def content_types(self):
        return self._content_types

    @property
    def routing_key_regexp(self):
        return self._routing_key_regexp


class Schema(object):
    def __init__(self, schema):
        self._content_types = {}
        self._queues = {}
        self._exchanges = {}
        self._protobuf_map = {}
        self._queue_name_map = {}
        self._exchange_name_map = {}
        self._load(schema)

    def _load(self, schema):
        for identifier, contentConfig in schema.content_types.iteritems():
            self._content_types[identifier] = ContentType(
                identifier,
                contentConfig['java_class'],
                contentConfig['python_class'],
                contentConfig['content_type'],
                contentConfig['x-protobuf']
            )

        # exchanges
        for identifier, exchangeConfig in schema.exchanges.iteritems():
            self._exchanges[identifier] = Exchange(
                identifier,
                exchangeConfig['name'],
                exchangeConfig['type'],
                exchangeConfig['durable'],
                exchangeConfig['auto_delete'],
                exchangeConfig['description'],
                exchangeConfig['content_types'],
                exchangeConfig['routing_key_regexp']
            )

        # queues
        for identifier, queueConfig in schema.queues.iteritems():
            self._queues[identifier] = Queue(
                identifier,
                queueConfig['name'],
                queueConfig['durable'],
                queueConfig['exclusive'],
                queueConfig['auto_delete'],
                queueConfig['description']
            )

            for config in queueConfig['bindings']:
                self._queues[identifier].bind(Binding(self.getExchange(config['exchange']), config['routing_key']))

        # Protobuf index
        for config in self._content_types.values():
            self._protobuf_map[config.protobuf_name] = config

        # Queue name index
        for config in self._queues.values():
            self._queue_name_map[config.name] = config

        # Exchange name index
        for config in self._exchanges.values():
            self._exchange_name_map[config.name] = config
                
    def getContentType(self, identifier):
        if isinstance(identifier, ContentType):
            return identifier
                    
        return self._content_types[identifier]

    def getExchange(self, name):
        if isinstance(name, Exchange):
            return name
            
        if name in self._exchange_name_map:
            return self._exchange_name_map[name]
        else:        
            return self._exchanges[name]
            
    def getQueue(self, name):
        if isinstance(name, Queue):
            return name
                    
        if name in self._queue_name_map:
            return self._queue_name_map[name]
        else:        
            return self._queues[name]

    def getQueues(self, queueNames=None):
        """
        Get all queues or if supplied, all queues matching a list of `queue_names`.
        
        @throws KeyError
        """
        if queueNames:
            queues = []
            for name in queueNames:
                if name in self._queue_name_map:
                    queues.append(self._queue_name_map[name])
                elif name in self._queues:
                    # Get by identifier
                    queues.append(self._queues[name])
                else:
                    raise KeyError('Queue "%s" does not exist.' % name)

            return queues
                
        return self._queues.values()
            
    def getExchanges(self):
        return self._exchanges.values()
                  
    def getProtobuf(self, protobuf_name):
        """
        Get a protobuf class from the identifier supplied.
        """
        if protobuf_name in self._protobuf_map:
            config = self._protobuf_map[protobuf_name]
        else:
            try:
                config = self.getContentType(protobuf_name)
            except KeyError:
                raise SchemaException('Could not find protobuf "%s"' % protobuf_name)

        try:
            cls = resolve(config.python_class)
        except ImportError:
            raise ImportError('Could not find protobuf python class "%s"' % config.python_class)
                                    
        return cls
        
    def getNewProtobuf(self, protobuf_name):
        """
        Will create a new instance of a protobuf from the identifier supplied.
        """
        cls = self.getProtobuf(protobuf_name)
                
        obj = cls()
        return obj

    def hydrateProtobuf(self, protobuf_name, content):
        """
        Given serialized protobuf string and a name, this will return the hyrdated
        protobuf
        """
        
        proto = self.getNewProtobuf(protobuf_name)
        proto.ParseFromString(content)
        return proto


# Load default schema on import
schema = Schema(queueschema)
getContentType = schema.getContentType
getExchange = schema.getExchange
getExchanges = schema.getExchanges
getQueue = schema.getQueue
getQueues = schema.getQueues
getProtobuf = schema.getProtobuf
getNewProtobuf = schema.getNewProtobuf
hydrateProtobuf = schema.hydrateProtobuf
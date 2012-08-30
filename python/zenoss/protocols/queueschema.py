##############################################################################
# 
# Copyright (C) Zenoss, Inc. 2010, all rights reserved.
# 
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
# 
##############################################################################


from zope.interface import implements
from zope.dottedname.resolve import resolve
from zenoss.protocols.interfaces import IContentType, IBinding, IQueue, IExchange, IQueueSchema
import re
import logging

log = logging.getLogger('zen.protocols')

__all__ = [
    'ContentType',
    'Binding',
    'Queue',
    'Exchange',
    'Schema',
]


class SchemaException(Exception):
    pass

# create readonly-ish objects that represent the configuration
class ContentType(object):
    implements(IContentType)

    def __init__(self, identifier, python_class):
        self.identifier = identifier
        self._python_class = python_class
        self._protobuf_name = None

    @property
    def python_class(self):
        return self._python_class

    @property
    def protobuf_name(self):
        if self._protobuf_name is None:
            self._protobuf_name = resolve(self._python_class).DESCRIPTOR.full_name
        return self._protobuf_name


class Binding(object):
    implements(IBinding)

    def __init__(self, exchange, routing_key, arguments=None):
        self._exchange = exchange
        self._routing_key = routing_key
        self._arguments = arguments

    @property
    def exchange(self):
        return self._exchange

    @property
    def routing_key(self):
        return self._routing_key

    @property
    def arguments(self):
        return self._arguments


class Queue(object):
    implements(IQueue)

    def __init__(self, identifier, name, durable, exclusive, auto_delete, description, arguments=None):
        self.identifier = identifier
        self._name = name
        self._durable = durable
        self._exclusive = exclusive
        self._auto_delete = auto_delete
        self._description = description
        self._arguments = arguments
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

    @property
    def arguments(self):
        return self._arguments

    def getBinding(self, exchangeIdentifier):
        return self._bindings[exchangeIdentifier]

class Exchange(object):
    implements(IExchange)

    def __init__(self, identifier, name, type, durable, auto_delete,
                 description, content_types, arguments=None, delivery_mode=2,
                 compression='none'):
        self.identifier = identifier
        self._name = name
        self._type = type
        self._auto_delete = auto_delete
        self._durable = durable
        self._description = description
        self._content_types = content_types
        self._arguments = arguments
        self._delivery_mode = delivery_mode
        self._compression = compression

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
    def arguments(self):
        return self._arguments

    @property
    def delivery_mode(self):
        return self._delivery_mode

    @property
    def compression(self):
        return self._compression


_REPLACEMENT_PATTERN = re.compile(r'\{([^}]+)\}')


class MissingReplacementException(Exception):
    pass

def substitute_replacements(template, replacements):
    if replacements is None:
        replacements = {}
    def _replace(matchobj):
        if matchobj.group(1) in replacements:
            return replacements[matchobj.group(1)]
        raise MissingReplacementException('Unable to replace %s in %s' % (matchobj.group(1), template))

    if '{' not in template or '}' not in template:
        return template

    return _REPLACEMENT_PATTERN.sub(_replace, template)

def substitute_replacements_in_arguments(arguments, replacements):
    substituted = {}
    for k, v in arguments.iteritems():
        substituted_key = substitute_replacements(k, replacements)
        if isinstance(v, basestring):
            substituted_value = substitute_replacements(v, replacements)
        else:
            substituted_value = v
        substituted[substituted_key] = substituted_value
    return substituted

class ExchangeNode(object):
    def __init__(self, identifier, name, type, durable, auto_delete, description, content_type_ids, arguments):
        self.identifier = identifier
        self.name = name
        self.type = type
        self.durable = durable
        self.auto_delete = auto_delete
        self.description = description
        self.content_type_ids = content_type_ids
        self.arguments = arguments

class QueueNode(object):
    def __init__(self, identifier, name, durable, exclusive, auto_delete, description, arguments):
        self.identifier = identifier
        self.name = name
        self.durable = durable
        self.exclusive = exclusive
        self.auto_delete = auto_delete
        self.description = description
        self.arguments = arguments
        self.binding_nodes = []

class BindingNode(object):
    def __init__(self, exchange_identifier, routing_key, arguments):
        self.exchange_identifier = exchange_identifier
        self.routing_key = routing_key
        self.arguments = arguments


class Schema(object):
    implements(IQueueSchema)

    def __init__(self, *schemas):
        self._content_types = {}
        self._queue_nodes = {}
        self._exchange_nodes = {}
        self._protobuf_full_name_to_class = None
        self._queue_nodes_by_name = {}
        self._exchange_nodes_by_name = {}
        self._properties = {}
        for schema in schemas:
            self._load(schema)

    def loadProperties(self, parsed):
        self._properties.update(parsed)

    def _getProperty(self, type_, identifier, key, default=None):
        locs = locals()
        return self._properties.get(
            "{type_}.{identifier}.{key}".format(**locs),
            self._properties.get(
                "{type_}.default.{key}".format(**locs),
                default))

    def _getExchangeProperty(self, identifier, key, default=None):
        return self._getProperty("exchange", identifier, key, default)

    def _getQueueProperty(self, identifier, key, default=None):
        return self._getProperty("queue", identifier, key, default)

    def _load(self, schema):
        for identifier, contentConfig in schema.get('content_types', {}).iteritems():
            content_type = ContentType(
                identifier,
                contentConfig['python_class']
            )
            self._content_types[identifier] = content_type

        # exchanges
        for identifier, exchangeConfig in schema.get('exchanges', {}).iteritems():
            exchange_node = ExchangeNode(
                identifier,
                exchangeConfig['name'],
                exchangeConfig['type'],
                exchangeConfig['durable'],
                exchangeConfig['auto_delete'],
                exchangeConfig['description'],
                exchangeConfig['content_types'],
                self._convertArguments(exchangeConfig.get('arguments', {}))
            )
            self._exchange_nodes[identifier] = exchange_node
            self._exchange_nodes_by_name[exchange_node.name] = exchange_node

        # queues
        for identifier, queueConfig in schema.get('queues', {}).iteritems():
            queue_node = QueueNode(
                identifier,
                queueConfig['name'],
                queueConfig['durable'],
                queueConfig['exclusive'],
                queueConfig['auto_delete'],
                queueConfig['description'],
                self._convertArguments(queueConfig.get('arguments', {}))
            )
            self._queue_nodes[identifier] = queue_node
            self._queue_nodes_by_name[queue_node.name] = queue_node

            for config in queueConfig.get('bindings', []):
                binding_node = BindingNode(config['exchange'],
                                           config['routing_key'],
                                           self._convertArguments(config.get('arguments', {})))
                queue_node.binding_nodes.append(binding_node)

    def _convertArguments(self, arguments):
        if arguments is None:
            return {}

        # TODO: amqplib doesn't support same types as Java client - for now just use
        # whatever types are returned from JSON parser
        return dict((k,v.get('value')) for k, v in arguments.iteritems())
                
    def getContentType(self, identifier):
        if isinstance(identifier, ContentType):
            return identifier
        
        return self._content_types[identifier]

    def getExchange(self, name, replacements=None):
        if isinstance(name, Exchange):
            return name

        if name in self._exchange_nodes:
            exchange_node = self._exchange_nodes[name]
        else:
            exchange_node = self._exchange_nodes_by_name[name]

        compression = self._getExchangeProperty(exchange_node, 'compression', 'none')

        return Exchange(exchange_node.identifier,
                        substitute_replacements(exchange_node.name, replacements),
                        exchange_node.type,
                        exchange_node.durable,
                        exchange_node.auto_delete,
                        exchange_node.description,
                        [self._content_types[content_type_id] for content_type_id in exchange_node.content_type_ids],
                        substitute_replacements_in_arguments(exchange_node.arguments, replacements),
                        int(self._getExchangeProperty(exchange_node.identifier, 'delivery_mode', 2)),
                        compression.lower()
                       )
            
    def getQueue(self, name, replacements=None):
        if isinstance(name, Queue):
            return name

        if name in self._queue_nodes:
            queue_node = self._queue_nodes[name]
        else:        
            queue_node = self._queue_nodes_by_name[name]

        arguments = substitute_replacements_in_arguments(queue_node.arguments,
                                                         replacements)

        ttl = self._getQueueProperty(queue_node.identifier, 'x-message-ttl', None)
        if ttl is not None:
            arguments['x-message-ttl'] = int(ttl)

        expires = self._getQueueProperty(queue_node.identifier, 'x-expires', None)
        if expires is not None:
            arguments['x-expires'] = int(expires)

        queue = Queue(queue_node.identifier,
                      substitute_replacements(queue_node.name, replacements),
                      queue_node.durable,
                      queue_node.exclusive,
                      queue_node.auto_delete,
                      queue_node.description,
                      arguments)

        for binding_node in queue_node.binding_nodes:
            binding = Binding(self.getExchange(binding_node.exchange_identifier, replacements),
                              substitute_replacements(binding_node.routing_key, replacements),
                              substitute_replacements_in_arguments(binding_node.arguments, replacements))
            queue.bind(binding)
        
        return queue

    def getProtobuf(self, protobuf_name):
        """
        Get a protobuf class from the identifier supplied.
        """

        # Initialize mapping of protobuf full name to protobuf class
        if self._protobuf_full_name_to_class is None:
            self._protobuf_full_name_to_class = {}
            for contentType in self._content_types.values():
                try:
                    cls = resolve(contentType.python_class)
                    self._protobuf_full_name_to_class[cls.DESCRIPTOR.full_name] = cls
                except ImportError:
                    log.exception('Failed to resolve protobuf: %s', protobuf_name)

        if protobuf_name in self._protobuf_full_name_to_class:
            cls = self._protobuf_full_name_to_class[protobuf_name]
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

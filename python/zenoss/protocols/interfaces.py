##############################################################################
# 
# Copyright (C) Zenoss, Inc. 2011, all rights reserved.
# 
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
# 
##############################################################################


from zope.interface import Interface, Attribute

class IContentType(Interface):
    """
    Interface representing content which is exchanged on the queues.
    """
    python_class = Attribute("The python class of the content type.")
    protobuf_name = Attribute("The full name of the protobuf message.")

class IBinding(Interface):
    """
    Interface representing
    """
    exchange = Attribute("The IExchange that this binding points to.")
    routing_key = Attribute("The routing key used to bind the queue to the exchange.")
    arguments = Attribute("The optional arguments used to create the binding.")

class IExchange(Interface):
    identifier = Attribute("The unique identifier for the exchange in the queue schema.")
    name = Attribute("The name of the exchange.")
    type = Attribute("The type of exchange (e.g. topic, fanout, headers).")
    durable = Attribute("True if the exchange is durable (persists after restart).")
    auto_delete = Attribute("True if the exchange should be deleted when it is no longer used (Deprecated).")
    description = Attribute("An optional description of the exchange.")
    content_types = Attribute("The content types (IContentType) which are published to the exchange.")
    arguments = Attribute("The optional arguments used to declare the exchange.")

class IQueue(Interface):
    identifier = Attribute("The unique identifier for the queue in the queue schema.")
    name = Attribute("The name of the queue.")
    durable = Attribute("True if the queue is durable (persists after restart).")
    exclusive = Attribute("True if only one consumer is allowed to consume from the queue.")
    auto_delete = Attribute("True if the queue should be deleted when it is no longer used.")
    description = Attribute("An optional description of the queue.")
    bindings = Attribute("All defined bindings (IBinding) of the queue to exchanges.")
    arguments = Attribute("The optional arguments used to declare the queue.")

class IQueueSchema(Interface):
    """
    Interface to retrieve defined queues, exchanges, and content types (typically loaded from a .qjs file).
    """

    def getContentType(identifier):
        """
        Returns the content type with the specified identifier, or None if it doesn't exist.

        @type identifier: str
        @param identifier: The unique identifier for the content type.
        @rtype: IContentType
        @return: The content type with the specified identifier.
        """

    def getExchange(identifier, replacements=None):
        """
        Returns the exchange with the specified identifier, or None if it doesn't exist.

        @type identifier: str
        @param identifier: The unique identifier for the exchange.
        @type replacements: dict
        @param replacements: An optional dictionary containing replacement values to be substituted in the exchange
                             configuration.
        @rtype: IExchange
        @return: The exchange matching the specified identifier.
        """

    def getQueue(identifier, replacements=None):
        """
        Returns the queue with the specified identifier, or None if it doesn't exist.

        @type identifier: str
        @param identifier: The unique identifier for the queue.
        @type replacements: dict
        @param replacements: An optional dictionary containing replacement values to be substituted in the queue
                             configuration.
        @rtype: IQueue
        @return: The queue with the specified identifier.
        """

    def getProtobuf(protobuf_name):
        """
        Returns the protobuf class matching the given content type identifier or protobuf message full name.

        @type protobuf_name: str
        @param protobuf_name: The content type unique identifier or the protobuf message full name.
        @rtype: Class of type google.protobuf.Message
        @return: The protobuf class.
        """

    def getNewProtobuf(protobuf_name):
        """
        Returns a newly instantiated protobuf message for the given content type identifier or protobuf message full
        name.

        @type protobuf_name: str
        @param protobuf_name: The content type unique identifier or the protobuf message full name.
        @rtype: google.protobuf.Message
        @return: A newly instantiated protobuf message.
        """

    def hydrateProtobuf(protobuf_name, content):
        """
        Returns a populated protobuf message with the specified content type identifier or protobuf message full name
        and encoded protobuf.

        @type protobuf_name: str
        @param protobuf_name: The content type unique identifier or the protobuf message full name.
        @type content: str
        @param content: The encoded protobuf message (as received on a queue for example).
        @rtype: google.protobuf.Message
        @return: A deserialized protobuf message of the specified type.
        """

class IAMQPConnectionInfo(Interface):
    host = Attribute("AMQP hostname")
    port = Attribute("AMQP port number")
    vhost = Attribute("AMQP virtual host")
    user = Attribute("AMQP username")
    password = Attribute("AMQP password")
    usessl = Attribute("AMQP SSL (True/False)")
    amqpconnectionheartbeat = Attribute("AMQP Connection Heart Beat in Seconds")

class IAMQPChannelAdapter(Interface):
    """
    Used to adapt the appropriate AMQP channel implementation and provide a way to declare a queue or
    exchange using the IQueue or IExchange returned from the queue schema.
    """

    def declareQueue(queue):
        """
        Creates the queue and binds it to the exchange(s).

        @type  queue: IQueue
        @param queue: The queue we wish to create.
        """

    def declareExchange(exchange):
        """
        Declares the exchange.

        @type  exchange: IExchange
        @param exchange: The exchange we wish to create.
        """

    def deleteQueue(queue):
        """
        Deletes and purges the queue).

        @type  queue: IQueue
        @param queue: The queue we wish to delete.
        @param queue: The queue we wish to delete.
        """

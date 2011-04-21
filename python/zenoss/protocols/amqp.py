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
import amqplib.client_0_8 as amqp
from amqplib.client_0_8.connection import Connection
from amqplib.client_0_8.basic_message import Message
from zenoss.protocols.amqpconfig import getAMQPConfiguration
from zenoss.protocols import queueschema
import socket

log = logging.getLogger('zen.%s' % __name__)

class Publisher(object):
    """
    A "blocking" way to publish Protobuf messages to an exchange.

    Example:

    with Publisher() as publish:
        publish(exchange, routing_key, obj)

    """
    def __init__(self):
        self._connection = None
        self._channel = None
        self._config = getAMQPConfiguration()
        self._exchanges = {}

    def __enter__(self):
        self.getChannel()
        return self.publish

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def getChannel(self):
        """
        Lazily initialize the connection
        """
        if not self._connection:
            self._connection = Connection(host='%s:%d' % (self._config.host, self._config.port),
                                          userid=self._config.user,
                                          password=self._config.password,
                                          virtual_host=self._config.vhost,
                                          ssl=self._config.usessl)
            log.debug("Connecting to RabbitMQ...")
        if not self._channel:
            self._channel = self._connection.connection.channel()

        return self._channel

    def useExchange(self, exchange):
        """
        Use an exchange, making sure we only declare it once per connection.
        """
        if exchange not in self._exchanges:
            exchangeConfig = queueschema.getExchange(exchange)
            self._exchanges[exchange] = exchangeConfig

            try:
                channel = self.getChannel()
                getAMQPConfiguration().declareExchange(channel, exchangeConfig)
            except Exception as e:
                log.exception(e)
                raise

        return self._exchanges[exchange]

    def close(self):
        try:
            if self._channel:
                self._channel.close()

            if self._connection:
                self._connection.close()
        except Exception as e:
            log.info("error closing publisher %s" % e)
        finally:
            self._reset()

    def _reset(self):
        self._channel = None
        self._connection = None
        self._exchanges = {}

    def publish(self, exchange, routing_key, obj, headers=None, mandatory=False):
        """
        Blocking method for publishing items to the queue

        @param obj: The message obj. This obj is expected to be a protobuf
        object and will be serialized to string before being sent.
        @type obj: <protobuf>
        @param exchange: The exchange to publish to.
        @type exchange: str
        @param routing_key: The routing key with which to publish `msg`.
        @type routing_key: str
        """
        msg = self.buildMessage(obj, headers)


        count = 0
        maxtries = 2
        while count < maxtries:
            count += 1
            try:
                channel = self.getChannel()
                exchangeConfig = self.useExchange(exchange)
                log.debug('Publishing with routing key %s to exchange %s' % (routing_key, exchangeConfig.name))
                channel.basic_publish(msg, exchangeConfig.name, routing_key, mandatory=mandatory)
                break
            except socket.error as e:
                log.info("amqp connection was closed %s" % e)
                self._reset()
            except Exception as e:
                log.exception(e)
                raise
        else:
            raise Exception("Could not publish message. Connection may be down")

    def buildMessage(self, obj, headers=None):
        msg_headers = {
            'X-Protobuf-FullName' : obj.DESCRIPTOR.full_name
        }

        if headers:
            msg_headers.update(headers)

        return Message(
            body=obj.SerializeToString(),
            content_type='application/x-protobuf',
            application_headers=msg_headers,
            delivery_mode=2 # Persist
        )

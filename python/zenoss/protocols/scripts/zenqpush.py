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
import sys
import pkg_resources # Import this so zenoss.protocols will be found
from json import loads
from zenoss.protocols import jsonformat
from zenoss.protocols.amqpconfig import AMQPConfig
from zenoss.protocols.data.queueschema import SCHEMA
from zenoss.protocols.queueschema import Schema
from zenoss.protocols.amqp import Publisher
from zenoss.protocols.exceptions import PublishException
from zenoss.protocols.scripts.scriptutils import initLogging, addLoggingOptions, get_zenpack_schemas
from google.protobuf.text_format import MessageToString
from amqplib.client_0_8.exceptions import AMQPException

log = logging.getLogger(__name__)

class Pusher(object):
    def __init__(self, exchange, messageType, schema, publisher):
        self.exchange = schema.getExchange(exchange)
        self.protobufClass = schema.getProtobuf(messageType)
        self.publisher = publisher

    def push(self, data, routingKey, mandatory=False, immediate=False):
        """
        Push a message on the queue

        @param data The data to populate the protobuf with
        """

        proto = self.protobufClass
        proto = jsonformat.from_dict(proto, data)

        log.info('Sending message of type "%s" to "%s" using key "%s"', proto.DESCRIPTOR.full_name, self.exchange.name, routingKey)
        log.debug('Message:\n    %s' % '\n    '.join(MessageToString(proto).split('\n')))

        published = False
        try:
            self.publisher.publish(self.exchange, routingKey, proto, mandatory=mandatory, immediate=immediate)
            published = True
        except PublishException, e:
            log.error("%s (%d)", e.reply_text, e.reply_code)
        except AMQPException, e:
            log.error("%s (%d)", e.amqp_reply_text, e.amqp_reply_code)
        return published

def main():
    from optparse import OptionParser
    parser = OptionParser(usage=usage())

    parser.add_option('-E', '--exchange', type='string', dest='exchange',
                      help="Exchange to push to", action='store')
    parser.add_option('-T', '--type', type='string', dest='messageType',
                      help="Type of message to create", action='store')
    parser.add_option('-R', '--routingkey', type='string', dest='routingKey',
                      help="Routing key for message", action='store')
    parser.add_option('-D', '--data', type='string', dest='data',
                      help="Message data as JSON, use '-' to read from stdin", action='store')
    parser.add_option('-M', '--mandatory', dest='mandatory',
                      help="Publish message with mandatory flag set.", action='store_true')
    parser.add_option('-I', '--immediate', dest='immediate',
                      help="Publish message with immediate flag set.", action='store_true')

    parser = AMQPConfig.addOptionsToParser(parser)
    parser = addLoggingOptions(parser)

    options, args = parser.parse_args()

    if not options.data:
        parser.error('You must supply input data.')
    elif not options.exchange:
        parser.error('You must supply an exchange.')
    elif not options.messageType:
        parser.error('You must supply a message type.')
    elif not options.routingKey:
        parser.error('You must supply a routing key.')

    schemas = [SCHEMA]
    schemas.extend(get_zenpack_schemas())

    amqpConnectionInfo = AMQPConfig()
    amqpConnectionInfo.update(options)
    schema = Schema(*schemas)
    publisher = Publisher(amqpConnectionInfo, schema)
    
    initLogging(options)

    pusher = Pusher(options.exchange, options.messageType, schema, publisher)

    if options.data == '-':
        data = loads(sys.stdin.read())
    else:
        data = loads(options.data)

    published = pusher.push(data=data, routingKey=options.routingKey, mandatory=options.mandatory,
                            immediate=options.immediate)
    if not published:
        sys.exit(1)

def usage():
    return """
    Push a message on to a RabbitMQ exchange.

    %prog [options]

    Example:

        %prog -u guest -p guest -H localhost -V / -E '$RawZenEvents' -T '$Event'  -R zenoss.zenevent.test -D '{ "uuid" : "123"}'
    """

if __name__ == "__main__":
    main()

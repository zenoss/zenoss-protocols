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

import logging
import sys
import pkg_resources # Import this so zenoss.protocols will be found
from json import loads
from time import time
from zenoss.protocols import queueschema, jsonformat
from zenoss.protocols.amqpconfig import AMQPConfig, getAMQPConfiguration
from zenoss.protocols.amqp import Publisher
from zenoss.protocols.scripts.zenqinit import Initializer, \
    initLogging, addLoggingOptions
from google.protobuf.text_format import MessageToString

log = logging.getLogger(__name__)

class Pusher(object):
    def __init__(self, exchange, messageType, publisher=None):
        self.exchange = queueschema.getExchange(exchange)
        self.protobufClass = queueschema.getProtobuf(messageType)

        if publisher:
            self.publisher = publisher
        else:
            self.publisher = Publisher()

    def push(self, data, routingKey):
        """
        Push a message on the queue

        @param Protobuf type The protobuf type to create
        @param dict data The data to populate the protobuf with
        """

        proto = self.protobufClass
        proto = jsonformat.from_dict(proto, data)

        if 'created_time' in proto.DESCRIPTOR.fields_by_name and not proto.HasField('created_time'):
            proto.created_time = int(time() * 1000)

        Initializer(self.publisher.getChannel()).init()

        log.info('Sending message of type "%s" to "%s" using key "%s"', proto.DESCRIPTOR.full_name, self.exchange.name, routingKey)
        log.debug('Message:\n    %s' % '\n    '.join(MessageToString(proto).split('\n')))



        return self.publisher.publish(self.exchange, routingKey, proto)

def usage():
    return """
    Push a message on to a RabbitMQ exchange.
    
    %prog [options]
    
    Example:
    
        %prog -u guest -p guest -H localhost -V / -E '$RawZenEvents' -T '$RawEvent'  -R zenoss.zenevent.test -D '{ "uuid" : "123"}'
    """    
    
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
        
    getAMQPConfiguration().update(options)
    initLogging(options)

    pusher = Pusher(exchange=options.exchange, messageType=options.messageType)

    if options.data == '-':
        data = loads(sys.stdin.read())
    else:
        data = loads(options.data)

    pusher.push(data=data, routingKey=options.routingKey)

if __name__ == "__main__":
    main()

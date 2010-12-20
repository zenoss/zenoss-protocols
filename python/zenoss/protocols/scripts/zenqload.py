
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
import pkg_resources # Import this so zenoss.protocols will be found
from zenoss.protocols.amqpconfig import AMQPConfig, getAMQPConfiguration
from zenoss.protocols.eventlet.amqp import getProtobufPubSub, Publishable
from zenoss.protocols.scripts.zenqdump import ProtobufStreamFormatter
from zenoss.protocols.scripts.zenqinit import Initializer, \
    initLogging, addLoggingOptions
from zenoss.protocols.jsonformat import to_json

log = logging.getLogger(__name__)

_FORMATTERS = {
    'protostream' : ProtobufStreamFormatter(),
}

class Loader(object):
    def __init__(self, stream, formatter, publisher=None):
        self.stream = stream
        self.formatter = formatter

        if publisher:
            self.publisher = publisher
        else:
            self.publisher = getProtobufPubSub(None)

    def load(self):
        Initializer(self.publisher.channel).init()

        for i, (exchange, routingKey, proto) in enumerate(self.formatter.read(self.stream)):
            log.info('Publishing message %d to %s with routing key %s' % (i + 1, exchange, routingKey))
            log.debug('Message: %s' % to_json(proto))

            self.publisher.publish(Publishable(
                message = proto,
                exchange = exchange,
                routingKey = routingKey,
                mandatory = True
            ))

def usage():
    return """
    Load messages into RabbitMQ.

    %prog [options]

    Example:

        %prog -u guest -p guest -H localhost -V / -Q '$RawZenEvents'
    """

def main():
    from optparse import OptionParser
    import sys
    parser = OptionParser(usage=usage())

    parser.add_option('-F', '--format', type='string', dest='format', default='protostream',
                       help='Format to dump the messages in (%s)' % ', '.join(_FORMATTERS.keys()))

    parser = AMQPConfig.addOptionsToParser(parser)
    parser = addLoggingOptions(parser)

    options, args = parser.parse_args()

    try:
        formatter = _FORMATTERS[options.format.lower()]
    except KeyError:
        parser.error('Invalid format "%s"' % options.format)

    getAMQPConfiguration().update(options)
    initLogging(options)

    loader = Loader(stream=sys.stdin, formatter=formatter)

    try:
        loader.load()
    except KeyboardInterrupt:
        pass

    loader.publisher.shutdown()

if __name__ == "__main__":
    main()

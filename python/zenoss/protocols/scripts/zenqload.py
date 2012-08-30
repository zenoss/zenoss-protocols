##############################################################################
# 
# Copyright (C) Zenoss, Inc. 2010, all rights reserved.
# 
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
# 
##############################################################################


import logging
import pkg_resources # Import this so zenoss.protocols will be found
from zenoss.protocols.amqpconfig import AMQPConfig
from zenoss.protocols.data.queueschema import SCHEMA
from zenoss.protocols.eventlet.amqp import getProtobufPubSub, Publishable
from zenoss.protocols.queueschema import Schema
from zenoss.protocols.scripts.zenqdump import ProtobufStreamFormatter
from zenoss.protocols.scripts.scriptutils import initLogging, addLoggingOptions, get_zenpack_schemas
from zenoss.protocols.jsonformat import to_json

log = logging.getLogger(__name__)

_FORMATTERS = {
    'protostream' : ProtobufStreamFormatter(),
}

class Loader(object):
    def __init__(self, stream, formatter, schema, publisher):
        self.stream = stream
        self.formatter = formatter
        self.schema = schema
        self.publisher = publisher

    def load(self):
        for i, (exchange, routingKey, proto) in enumerate(self.formatter.read(self.schema, self.stream)):
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

    parser.add_option('-c', '--compression', type='string', dest='compression', default='none',
                     help='Message compression algorithm (possible values: deflate, none)')

    parser = AMQPConfig.addOptionsToParser(parser)
    parser = addLoggingOptions(parser)

    options, args = parser.parse_args()

    schemas = [SCHEMA]
    schemas.extend(get_zenpack_schemas())

    try:
        formatter = _FORMATTERS[options.format.lower()]
    except KeyError:
        parser.error('Invalid format "%s"' % options.format)

    initLogging(options)

    amqpConnectionInfo = AMQPConfig()
    amqpConnectionInfo.update(options)
    schema = Schema(*schemas)
    schema.loadProperties({'exchange.default.compression': options.compression.lower()})
    publisher = getProtobufPubSub(amqpConnectionInfo, schema, None)


    loader = Loader(sys.stdin, formatter, schema, publisher)

    try:
        loader.load()
    except KeyboardInterrupt:
        pass

    loader.publisher.shutdown()

if __name__ == "__main__":
    main()

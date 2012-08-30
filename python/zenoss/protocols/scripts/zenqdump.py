##############################################################################
# 
# Copyright (C) Zenoss, Inc. 2010, all rights reserved.
# 
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
# 
##############################################################################
import zlib
import logging
import pkg_resources # Import this so zenoss.protocols will be found
from zenoss.protocols.queueschema import Schema, SchemaException
from zenoss.protocols.data.queueschema import SCHEMA
from zenoss.protocols.amqpconfig import AMQPConfig
from zenoss.protocols import hydrateQueueMessage
from zenoss.protocols.amqp import Publisher
from zenoss.protocols.scripts.scriptutils import initLogging, addLoggingOptions, get_zenpack_schemas
from zenoss.protocols.jsonformat import to_json
from struct import pack, unpack
from base64 import b64encode
import select
from amqplib.client_0_8.exceptions import AMQPException

log = logging.getLogger(__name__)

HEADER_CONTENT_TYPE = 'Content-Type'
HEADER_CONTENT_LENGTH = 'Content-Length'
HEADER_CONTENT_TRANSFER_ENCODING = 'Content-Transfer-Encoding'
HEADER_QUEUE_NAME = 'X-Queue-Name'
HEADER_EXCHANGE_NAME = 'X-Exchange-Name'
HEADER_ROUTING_KEY = 'X-Routing-Key'
HEADER_DELIVERY_MODE = 'X-Delivery-Mode'
HEADER_PRIORITY = 'X-Priority'
HEADER_REDELIVERED = 'X-Redelivered'
HEADER_MESSAGE_COUNT = 'X-Message-Count'
HEADER_DELIVERY_TAG = 'X-Delivery-Tag'
HEADER_ORIGINAL_CONTENT_TYPE = 'X-Original-Content-Type'
HEADER_ORIGINAL_CONTENT_ENCODING = 'X-Original-Content-Encoding'
HEADER_PROTOBUF_FULL_NAME = 'X-Protobuf-FullName'

CONTENT_TYPE_JSON = 'application/json'
CONTENT_TYPE_PROTOBUF = 'application/x-protobuf'

class FormatError(Exception):
    pass

class Formatter(object):
    def dump(self, message, schema, queueName, stream):
        raise NotImplementedError()

    def dumpHeader(self, key, value, stream):
        stream.write('%s: %s\n' % (key, value))

    def dumpHeaders(self, message, stream):
        for k, v in message.application_headers.iteritems():
            self.dumpHeader(k, v, stream)

    def dumpCommonHeaders(self, message, stream):
        routing_key = message.delivery_info['routing_key']
        exchange = message.delivery_info['exchange']
        self.dumpHeader(HEADER_EXCHANGE_NAME, exchange, stream)
        self.dumpHeader(HEADER_ROUTING_KEY, routing_key, stream)

        message_count = message.delivery_info.get('message_count')
        if message_count is not None:
            self.dumpHeader(HEADER_MESSAGE_COUNT, message_count, stream)

        redelivered = message.delivery_info.get('redelivered')
        if redelivered is not None:
            self.dumpHeader(HEADER_REDELIVERED, redelivered, stream)

        delivery_tag = message.delivery_info.get('delivery_tag')
        if delivery_tag is not None:
            self.dumpHeader(HEADER_DELIVERY_TAG, delivery_tag, stream)

        contentType = message.properties.get('content_type')
        if contentType is not None:
            self.dumpHeader(HEADER_ORIGINAL_CONTENT_TYPE, contentType, stream)

        delivery_mode = message.properties.get('delivery_mode')
        if delivery_mode is not None:
            self.dumpHeader(HEADER_DELIVERY_MODE, delivery_mode, stream)

        priority = message.properties.get('priority')
        if priority is not None:
            self.dumpHeader(HEADER_PRIORITY, priority, stream)

        content_encoding = message.properties.get('content_encoding')
        if content_encoding is not None:
            self.dumpHeader(HEADER_ORIGINAL_CONTENT_ENCODING, content_encoding, stream)

class JsonFormatter(Formatter):
    """
    Outputs HTTP style headers with information about the queue state of the message
    along with a JSON formatted message.
    """
    def dump(self, message, schema, queueName, stream):
        try:
            proto = hydrateQueueMessage(message, schema)
        except SchemaException:
            log.error("Unable to hydrate protobuf %s with headers %s " % (message.body, message.properties.get('application_headers')))
            return

        self.dumpHeaders(message, stream)
        self.dumpHeader(HEADER_QUEUE_NAME, queueName, stream)
        self.dumpCommonHeaders(message, stream)
        self.dumpHeader(HEADER_CONTENT_TYPE, CONTENT_TYPE_JSON, stream)

        data = to_json(proto, indent=2)
        self.dumpHeader(HEADER_CONTENT_LENGTH, len(data), stream)

        stream.write('\n')
        stream.write(data)
        stream.write('\n\n')

class ProtobufFormatter(Formatter):
    """
    Outputs HTTP style headers with information about the queue state of the message
    along with a base64 encoded protobuf message.
    """
    def dump(self, message, schema, queueName, stream):
        try:
            # Make sure we can read it in, then convert back to string
            proto = hydrateQueueMessage(message, schema)
        except SchemaException:
            log.error("Unable to hydrate protobuf %s with headers %s " % (message.body, message.properties.get('application_headers')))
            return

        self.dumpHeaders(message, stream)
        self.dumpHeader(HEADER_QUEUE_NAME, queueName, stream)
        self.dumpCommonHeaders(message, stream)

        self.dumpHeader(HEADER_CONTENT_TYPE, CONTENT_TYPE_PROTOBUF, stream)
        self.dumpHeader(HEADER_CONTENT_TRANSFER_ENCODING, 'base64', stream)

        data = b64encode(proto.SerializeToString())
        self.dumpHeader(HEADER_CONTENT_LENGTH, len(data), stream)

        stream.write('\n')
        stream.write(data)
        stream.write('\n\n')

class ProtobufStreamFormatter(Formatter):
    """
    A binary stream of protobufs.
    """

    def _write_shortstr(self, stream, s, encoding='utf-8'):
        """
        Write an integer as an unsigned 8-bit value and
        Write a string up to 255 bytes long (after any encoding).

        If passed a unicode string, encode with the specified
        encoding (defaults to utf-8).
        """
        if isinstance(s, unicode):
            s = s.encode(encoding)

        size = len(s)
        if size > 255:
            raise ValueError('String "%s" too long (%d)' % (s, size))
        stream.write(pack('B%ds' % size, size, s))

    def _read_shortstr(self, stream, lenByte=None, encoding='utf-8'):
        """
        Read a short string that's stored in up to 255 bytes. Return
        it as a decoded Python unicode object, under the encoding
        passed as 'encoding', or as a byte string if 'encoding' is None.

        """
        if not lenByte:
            lenByte = stream.read(1)
        size = unpack('B', lenByte)[0]
        raw = stream.read(size)
        if encoding:
            return raw.decode(encoding)
        return raw

    def dump(self, message, schema, queueName, stream):
        # Make sure we can read it in, then convert back to string
        routing_key = message.delivery_info['routing_key']
        exchange = message.delivery_info['exchange']
        proto = hydrateQueueMessage(message, schema)
        type = proto.DESCRIPTOR.full_name

        payload = proto.SerializeToString()
        log.debug(to_json(proto))
        self._write_shortstr(stream, type)
        self._write_shortstr(stream, exchange)
        self._write_shortstr(stream, routing_key)

        size = len(payload)
        stream.write(pack('>I%dsB' % size, size, payload, 0xce))


    def read(self, schema, stream):
        while True:
            r, w, x = select.select([stream], [], [])
            if r and r[0] == stream:
                byte = stream.read(1)
                if not byte:
                    break

                type = self._read_shortstr(stream, byte)
                exchange = self._read_shortstr(stream)
                routing_key = self._read_shortstr(stream)

                size = unpack('>I', stream.read(4))[0]
                payload = stream.read(size)
                ch = stream.read(1)
                if ch != '\xce':
                    raise FormatError('Received 0x%02x while expecting 0xce' % ord(ch))

                yield exchange, routing_key, schema.hydrateProtobuf(type, payload)


_FORMATTERS = {
    'json' : JsonFormatter(),
    'proto' : ProtobufFormatter(),
    'protostream' : ProtobufStreamFormatter(),
}

class Dumper(object):
    def __init__(self, stream, formatter, channel, schema, acknowledge=False, skip=False):
        self.acknowledge = acknowledge
        self.stream = stream
        self.formatter = formatter
        self.skip = skip
        self.channel = channel
        self.schema = schema

    def dumpMessage(self, msg, queueName):
        """
        Get the string form of the message.
        """
        # display the protobuf
        if msg.properties.get('content_encoding', None) == 'deflate':
            msg.body = zlib.decompress(msg.body)
        try:
            self.formatter.dump(msg, self.schema, queueName, self.stream)
        except FormatError as e:
            # just display the message body
            log.error('Could not format message body: %s' % e)
            log.debug(msg.body)

    def dumpQueue(self, queueName, limit=None):
        try:
            msg = self.channel.basic_get(queueName)
        except AMQPException, e:
            log.error("%s (%s)", e.amqp_reply_text, e.amqp_reply_code)
            return
        if msg:
            # read everything from the queue
            log.info("Receiving messages from queue: %s (%d available)", queueName,
                     msg.delivery_info['message_count']+1)
            num = 0
            while msg:
                num += 1
                if not self.skip:
                    log.info('Dumping message %s' % msg.delivery_info['delivery_tag'])
                    self.dumpMessage(msg, queueName)

                if self.acknowledge:
                    log.info('Acknowledging message %s' % msg.delivery_info['delivery_tag'])
                    self.channel.basic_ack(msg.delivery_info['delivery_tag'])

                if limit and num >= limit:
                    log.info('Matched dump limit of %d items' % limit)
                    return

                msg = self.channel.basic_get(queueName)
        else:
            log.info("Queue %s is empty", queueName)

    def dumpQueues(self, queueNames, limit=None):
        """
        This dumps the specified queues.
        """
        for queueName in queueNames:
            self.dumpQueue(queueName, limit)

def usage():
    return """
    Dump the messages in a RabbitMQ queue to the screen.

    %prog [options]

    Example:

        %prog -u guest -p guest -H localhost -V / zenoss.queues.zep.rawevents
    """

def main():
    from optparse import OptionParser
    import sys
    parser = OptionParser(usage=usage())

    parser.add_option("-A", '--ack', action="store_true", dest="acknowledge",
                       help="Acknowledge the message, acknowledging a message will remove it from the queue")
    parser.add_option('-F', '--format', type='string', dest='format', default='json',
                       help='Format to dump the messages in (%s)' % ', '.join(_FORMATTERS.keys()))
    parser.add_option('-M', '--max', type='int', dest='max_items',
                       help='Maximum items to dump')
    parser.add_option('-S', '--skip', action="store_true", dest="skip",
                      help="Skip processing messages on the queue - use with --ack to clear a queue.")

    parser = AMQPConfig.addOptionsToParser(parser)
    parser = addLoggingOptions(parser)

    options, args = parser.parse_args()
    if not args:
        parser.error("Require one or more queues as arguments")
    
    if options.skip and not options.acknowledge:
        parser.error("Option --skip must be used with --ack")

    schemas = [SCHEMA]
    schemas.extend(get_zenpack_schemas())

    amqpConnectionInfo = AMQPConfig()
    amqpConnectionInfo.update(options)
    schema = Schema(*schemas)

    try:
        formatter = _FORMATTERS[options.format.lower()]
    except KeyError:
        parser.error('Invalid format "%s"' % options.format)

    initLogging(options)

    publisher = Publisher(amqpConnectionInfo, schema)
    dumper = Dumper(sys.stdout, formatter, publisher.getChannel(), schema, acknowledge=options.acknowledge,
                    skip=options.skip)
    dumper.dumpQueues(args, limit=options.max_items)

if __name__ == "__main__":
    main()

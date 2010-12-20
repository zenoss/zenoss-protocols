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
from zenoss.protocols.queueschema import schema
from zenoss.protocols.amqpconfig import AMQPConfig, getAMQPConfiguration
from zenoss.protocols import hydrateQueueMessage
from zenoss.protocols.amqp import Publisher
from zenoss.protocols.scripts.zenqinit import Initializer, \
    initLogging, addLoggingOptions
from zenoss.protocols.jsonformat import to_json
from zenoss.protocols import queueschema
from struct import pack, unpack
from base64 import b64encode
import select

log = logging.getLogger(__name__)

class FormatError(Exception):
    pass

class Formatter(object):
    def dump(self, message, queueName, stream):
        raise NotImplementedError()

    def dumpHeader(self, key, value, stream):
        stream.write('%s: %s\n' % (key, value))

    def dumpHeaders(self, message, stream):
        for k, v in message.application_headers.iteritems():
            self.dumpHeader(k, v, stream)

class JsonFormatter(Formatter):
    """
    Outputs HTTP style headers with information about the queue state of the message
    along with a JSON formatted message.
    """
    def dump(self, message, queueName, stream):
        proto = hydrateQueueMessage(message)

        self.dumpHeaders(message, stream)
        self.dumpHeader('X-Queue-Name', queueName, stream)

        contentType = message.properties.get('content_type', None)
        if contentType:
            self.dumpHeader('X-Original-Content-Type', contentType, stream)

        self.dumpHeader('Content-Type', 'application/json', stream)

        data = to_json(proto)
        self.dumpHeader('Content-Length', len(data), stream)

        stream.write('\n')
        stream.write(data)
        stream.write('\n\n')

class ProtobufFormatter(Formatter):
    """
    Outputs HTTP style headers with information about the queue state of the message
    along with a base64 encoded protobuf message.
    """
    def dump(self, message, queueName, stream):
        # Make sure we can read it in, then convert back to string
        proto = hydrateQueueMessage(message)

        self.dumpHeaders(message, stream)
        self.dumpHeader('X-Queue-Name', queueName, stream)

        contentType = message.properties.get('content_type', None)
        if contentType != 'application/x-protobuf':
            self.dumpHeader('X-Original-Content-Type', contentType, stream)

        self.dumpHeader('Content-Type', 'application/x-protobuf', stream)
        self.dumpHeader('Content-Transfer-Encoding', 'base64', stream)

        data = b64encode(proto.SerializeToString())
        self.dumpHeader('Content-Length', len(data), stream)

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

    def dump(self, message, queueName, stream):
        # Make sure we can read it in, then convert back to string
        routing_key = message.delivery_info['routing_key']
        exchange = message.delivery_info['exchange']
        proto = hydrateQueueMessage(message)
        type = proto.DESCRIPTOR.full_name

        payload = proto.SerializeToString()
        log.debug(to_json(proto))
        self._write_shortstr(stream, type)
        self._write_shortstr(stream, exchange)
        self._write_shortstr(stream, routing_key)

        size = len(payload)
        stream.write(pack('>I%dsB' % size, size, payload, 0xce))


    def read(self, stream):
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

                yield exchange, routing_key, queueschema.hydrateProtobuf(type, payload)


_FORMATTERS = {
    'json' : JsonFormatter(),
    'proto' : ProtobufFormatter(),
    'protostream' : ProtobufStreamFormatter(),
}

class Dumper(object):
    def __init__(self, stream, formatter, channel=None, acknowledge=False):
        self.acknowledge = acknowledge
        self.stream = stream
        self.formatter = formatter

        if channel:
            self.channel = channel
        else:
            self.channel = Publisher().getChannel()

    def dumpMessage(self, msg, queueName):
        """
        Get the string form of the message.
        """
        # display the protobuf
        try:
            self.formatter.dump(msg, queueName, self.stream)
        except FormatError as e:
            # just display the message body
            log.error('Could not format message body: %s' % e)
            log.debug(msg.body)

        if self.acknowledge:
            log.info('Acknowledging message %s' % msg.delivery_info['delivery_tag'])
            self.channel.basic_ack(msg.delivery_info['delivery_tag'])

    def dumpQueue(self, queueConfig, limit=None):
        """
        This binds the queue to the exchange and then attempts to read all
        the messages off of the queue. The messages are never acknowledged
        so they can be read multiple times
        """
        qname = queueConfig.name
        msg = self.channel.basic_get(qname)
        if msg:
            # read everything from the queue
            log.info("Receiving messages from queue: %s", qname)
            num = 0
            while msg:
                num += 1
                log.info('Dumping message %s' % msg.delivery_info['delivery_tag'])
                self.dumpMessage(msg, qname)

                if limit and num >= limit:
                    log.info('Matched dump limit of %d items' % limit)
                    return

                msg = self.channel.basic_get(qname)

        log.info("Queue %s is empty", qname)

    def dumpQueues(self, queueNames=None, limit=None):
        """
        This sets up the AMQP connection, declares the exchanges and then
        proceeds to dump each queue
        """
        Initializer(self.channel).init()

        # dump queues
        for queueConfig in schema.getQueues(queueNames):
            self.dumpQueue(queueConfig, limit)

def usage():
    return """
    Dump the messages in a RabbitMQ queue to the screen.

    %prog [options]

    Example:

        %prog -u guest -p guest -H localhost -V / -Q '$RawZenEvents'
    """

def main():
    from optparse import OptionParser
    import sys
    parser = OptionParser(usage=usage())

    parser.add_option('-Q', '--queues', type='string', dest='queues',
                      help="Queue to connect to", action='append')
    parser.add_option("-A", '--ack', action="store_true", dest="acknowledge",
                       help="Acknowledge the message, acknowledging a message will remove it from the queue")

    parser.add_option('-F', '--format', type='string', dest='format', default='json',
                       help='Format to dump the messages in (%s)' % ', '.join(_FORMATTERS.keys()))

    parser.add_option('-L', '--list', action='store_true', dest='list',
                       help='List the availible queues')

    parser.add_option('-M', '--max', type='int', dest='max_items',
                       help='Maximum items to dump')

    parser = AMQPConfig.addOptionsToParser(parser)
    parser = addLoggingOptions(parser)

    options, args = parser.parse_args()

    if options.list:
        for queue in queueschema.getQueues():
            print '%s - %s' % (queue.identifier, queue.name)
        return

    try:
        formatter = _FORMATTERS[options.format.lower()]
    except KeyError:
        parser.error('Invalid format "%s"' % options.format)

    getAMQPConfiguration().update(options)
    initLogging(options)

    dumper = Dumper(acknowledge=options.acknowledge, stream=sys.stdout, formatter=formatter)
    dumper.dumpQueues(queueNames=options.queues, limit=options.max_items)

if __name__ == "__main__":
    main()

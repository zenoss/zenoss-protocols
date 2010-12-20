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
from zenoss.protocols import queueschema
from zenoss.protocols.amqp import Publisher
from zenoss.protocols.amqpconfig import AMQPConfig, getAMQPConfiguration

log = logging.getLogger(__name__)

class Initializer(object):
    def __init__(self, channel=None):
        if channel:
            self.channel = channel
        else:
            self.channel = Publisher().getChannel()

    def declareExchanges(self):
        """
        Makes sure the exchange is declared before we try to bind a queue to it
        """
        config = getAMQPConfiguration()
        config.declareExchanges(self.channel)

    def declareQueues(self):
        """
        Sets up each queue and bindings to the exchanges.
        """
        config = getAMQPConfiguration()
        config.declareQueues(self.channel)

    def deleteExchanges(self):
        """
        Deletes every exchange defined in our schema. It keeps
        track of which items have been deleted so it does not try to
        delete an exchange that doesn't exist since if you delete
        an exchange that does not exist the channel connection
        is left in an invalid state
        """
        deleted_exchanges = set()
        for exchangeConfig in queueschema.getExchanges():
            name = exchangeConfig.name
            log.info("Removing exchange %s" % name)
            if not name in deleted_exchanges:
                self.channel.exchange_delete(name)
                deleted_exchanges.add(name)

    def deleteQueues(self):
        """
        Deletes every queue defined in our schema. It keeps
        track of which items have been deleted so it does not try to
        delete a queue that doesn't exist since if you delete
        a queue that does not exist the channel connection
        is left in an invalid state
        """
        deleted_queues = set()
        for queueConfig in queueschema.getQueues():
            name = queueConfig.name
            log.info("Removing queue %s" % name)
            if not name in deleted_queues:
                self.channel.queue_delete(queueConfig.name)
                deleted_queues.add(name)

    def reset(self):
        """
        Delete all the exchanges and queues and re-declare them.
        """
        # if we reset the queues we want to make sure they
        # are all set up correctly first since trying to
        # delete a queue that doesn't exist will throw an exception
        self.init()

        self.deleteQueues()
        self.deleteExchanges()

        # reinitialize them
        self.init()

    def init(self):
        """
        Goes through and tells the amqp server to create each exchange
        and queue defined in our schema
        """
        # if we are reset don't mention initializing
        self.declareExchanges()
        self.declareQueues()

def initLogging(options):
    if options.quiet:
       level = logging.ERROR
    elif options.debug:
       level = logging.DEBUG
    else:
       level = logging.INFO

    logging.basicConfig(
        level=level,
        format='%(asctime)s %(levelname)-6s: %(message)s',
        stream=sys.stderr
    )
    logging.getLogger('').setLevel(level)

def addLoggingOptions(parser):
    parser.add_option("-q", "--quiet", action="store_true", dest="quiet",
                    help="No logging messages will be displayed", default=False)
    parser.add_option("-d", "--debug", action="store_true", dest="debug",
                    help="Verbose logging", default=False)
    return parser

def usage():
    return """
    Initialize all the Zenoss RabbitMQ exchange and queues.

    %prog [options]

    Example:

        %prog -u guest -p guest -H localhost -V /
    """

def main():
    from optparse import OptionParser
    parser = OptionParser(usage=usage())

    parser.add_option('-R', "--reset", action="store_true", dest="reset",
                      help="Will remove queues and exchanges and re-declare them", default=False)

    parser = AMQPConfig.addOptionsToParser(parser)
    parser = addLoggingOptions(parser)

    options, args = parser.parse_args()

    getAMQPConfiguration().update(options)
    initLogging(options)

    initializer = Initializer()

    if options.reset:
        initializer.reset()
    else:
        initializer.init()

if __name__ == "__main__":
    main()

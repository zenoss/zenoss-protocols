###########################################################################
#
# This program is part of Zenoss Core, an open source monitoring platform.
# Copyright (C) 2011, Zenoss Inc.
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License version 2 or (at your
# option) any later version as published by the Free Software Foundation.
#
# For complete information please visit: http://www.zenoss.com/oss/
#
###########################################################################

class PublishException(Exception):
    """
    Generic exception sent when a message is published to a queue with
    either the mandatory or immediate flags set to true and the message
    fails to publish.
    """
    def __init__(self, reply_code, reply_text, exchange, routing_key):
        super(PublishException, self).__init__()
        self._reply_code = reply_code
        self._reply_text = reply_text
        self._exchange = exchange
        self._routing_key = routing_key

    @property
    def reply_code(self):
        return self._reply_code

    @property
    def reply_text(self):
        return self._reply_text

    @property
    def exchange(self):
        return self._exchange

    @property
    def routing_key(self):
        return self._routing_key

    def __str__(self):
        return "Reply code: %d, Reply text: %s, Exchange: %s, Routing key: %s" % (
            self.reply_code, self.reply_text, self.exchange, self.routing_key
        )

class NoRouteException(PublishException):
    """
    Exception sent when the mandatory flag is specified, but there
    is no route from the exchange to a queue. This flag indicates that
    the message has been lost and will never be delivered to the
    queue.
    """
    pass

class NoConsumersException(PublishException):
    """
    Exception sent when the immediate flag is specified, but there
    are no active consumers for the message. This means that the
    consumer which processes the queue is not currently active.
    """
    pass
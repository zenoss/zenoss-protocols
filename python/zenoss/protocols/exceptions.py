##############################################################################
# 
# Copyright (C) Zenoss, Inc. 2011, all rights reserved.
# 
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
# 
##############################################################################


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

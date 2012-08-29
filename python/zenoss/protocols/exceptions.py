##############################################################################
# 
# Copyright (C) Zenoss, Inc. 2011, all rights reserved.
# 
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
# 
##############################################################################

from txamqp.client import Closed
from amqplib.client_0_8.exceptions import AMQPChannelException


class ConnectionError(IOError):
    """
    Exception raised when the connection to the queue server fails.
    """

    def __init__(self, message, exc):
        """
        Initialize an instance of ConnectionError.

        @param message {str} Relevant error message.
        @param exc {socket.error} Exception object.
        """
        super(ConnectionError, self).__init__(exc.errno, exc.strerror)
        self.message = message

    def __str__(self):
        return "%s: [%s] %s" % (self.message, self.errno, self.strerror)



class ChannelClosedError(IOError):
    """
    Wraps txamqp/amqplib exceptions
    """
    _replyCode = None

    def __init__(self, exc):
        super(ChannelClosedError, self).__init__(exc)
        if isinstance(exc, Closed):
            self._replyCode = exc.args[0].fields[0]
        elif isinstance(exc, AMQPChannelException):
            self._replyCode = exc.amqp_reply_code

    @property
    def replyCode(self):
        return self._replyCode



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

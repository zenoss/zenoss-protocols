from zenoss.protocols.twisted import amqp
from zenoss.protocols.twisted.amqp import AMQPFactory

from zope.interface import implements
from zenoss.protocols.interfaces import IAMQPConnectionInfo, IQueueSchema
from zenoss.protocols.queueschema import Schema
from fixtures import queueschema
from txamqp.client import Closed

from mock import MagicMock
from mock import patch

from twisted.internet import defer, reactor
from twisted.internet.task import Clock
from twisted.trial import unittest
from twisted.test import proto_helpers


class ConnectionInfo ():
    implements(IAMQPConnectionInfo)

    def __init__(self):
        self.vhost = '/zenoss'
        self.host = '127.0.0.1'
        self.port = 5672
        self.usessl = False
        self.amqpconnectionheartbeat = MagicMock(return_value=300)
        self.user = 'test_user'
        self.password = 'test_pass'


def BOMB(reason):
    raise RuntimeError("BOMB {}".format(reason))


def lightBomb(fuse, message, reactor=reactor):
    d = defer.Deferred()
    reactor.callLater(fuse, d.callback, message)
    return d


class BombTest(unittest.TestCase):

    timeout = 3

    def test_explodes(self):

        def check(din):
            self.assertEqual(din, 'exploded')
            print("I was executed with %r" % din)

        clock = Clock()
        d = lightBomb(1, 'exploded', reactor=clock)
        d.addCallback(check)
        clock.advance(2)
        return d


def test_errback(din):
    print('test_errback caught exception')
    din.printTraceback()
    return din


class AMQPProtocolTestCase(unittest.TestCase):

    def setUp(self):
        self.tr = proto_helpers.StringTransport()
        self.tr.socket = MagicMock()
        self.connection_info = ConnectionInfo()
        self.reactor = Clock()
        self.reactor.connectTCP = MagicMock()
        self.factory = AMQPFactory(self.connection_info,
                                   queueschema,
                                   reactor=self.reactor)
        self.proto = self.factory.buildProtocol(('127.0.0.1', 0, self.reactor))
        self.tr.proto = self.proto
        self.proto.makeConnection(self.tr)
        self.reactor.advance(1)

    def tearDown(self):
        self.factory.shutdown()

    def test_connectionMade(self):
        print("proto._connected is %s" % self.proto._connected)
        self.tr.proto.start = MagicMock(spec=self.proto.start)
        chan = object()
        self.proto.get_channel = MagicMock(spec=self.proto.get_channel,
                                           return_value=chan)

        cm = self.proto.connectionMade()

        self.reactor.advance(3)

        cm.addCallback(
            self.assertTrue,
            self.proto._connected
        )
        cm.addCallback(
            self.assertTrue, (chan is self.proto.chan)
        )
        cm.addCallback(
            self.assertTrue, (self.tr.proto.get_channel.call_count == 1)
        )

        return cm

    @patch('zenoss.protocols.twisted.amqp.ReconnectingClientFactory.clientConnectionFailed',
           autospec=True)
    def test_connectionMade_authentication_errors(self, m_ccf):
        self.tr.proto.start = MagicMock(spec=self.proto.start)
        chan = object()
        self.proto.get_channel = MagicMock(spec=self.proto.get_channel,
                                           return_value=chan)

        def raise_err(din):
            raise Closed()
        self.factory.onAuthenticated = raise_err

        cm = self.proto.connectionMade()
        cm.addErrback(test_errback)
        self.reactor.advance(4)
        print(cm.result)

        # reconnecting client failure should be called if the connectoin fails
        self.assertEqual(m_ccf.call_count, 1,
                         "clientConnectionFailed was not called")
        self.assertFailure(cm, Closed)

        return cm

    @patch('zenoss.protocols.twisted.amqp.ReconnectingClientFactory.clientConnectionFailed',
           autospec=True)
    def test_connectionMade_handles_errors(self, m_ccf):
        self.tr.proto.start = MagicMock(spec=self.proto.start)
        chan = object()
        self.proto.get_channel = MagicMock(spec=self.proto.get_channel,
                                           return_value=chan)

        self.factory.onAuthenticated = BOMB

        cm = self.proto.connectionMade()
        cm.addErrback(test_errback)
        self.reactor.advance(4)
        print(cm.result)

        # reconnecting client failure should be called if the connectoin fails
        self.assertEqual(m_ccf.call_count, 1,
                         "clientConnectionFailed was not called")
        self.assertFailure(cm, RuntimeError)

        return cm


class AMQPFactoryTestCase(unittest.TestCase):

    def setUp(self):
        self.tr = proto_helpers.StringTransport()
        self.tr.socket = MagicMock()

        self.connection_info = ConnectionInfo()
        self.reactor = Clock()
        self.reactor.connectTCP = MagicMock()
        self.factory = AMQPFactory(self.connection_info,
                                   queueschema,
                                   reactor=self.reactor)
        self.proto = self.factory.buildProtocol(('127.0.0.1', 0, self.reactor))
        self.tr.proto = self.proto
        self.proto.makeConnection(self.tr)
        self.reactor.advance(1)

    def tearDown(self):
        self.factory.shutdown()

    def test_buildProtocol(self):
        self.assertEqual(
            self.proto.__class__.__name__,
            self.factory.protocol.__name__
        )

    def test_onAuthenticated(self):
        d = self.factory.onAuthenticated('test')
        self.reactor.advance(2)
        print(d.result)
        self.assertTrue(d.called)
        return d

    def test_default_errback(self):
        d = defer.Deferred()
        d.addCallback(BOMB)
        d.addErrback(self.factory._defaultErrback)

        test = self.assertFailure(d, RuntimeError)
        test.callback("go")
        return test

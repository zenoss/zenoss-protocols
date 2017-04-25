from zenoss.protocols.twisted import amqp
from zenoss.protocols.twisted.amqp import AMQPFactory, IAMQPChannelAdapter

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


INVALIDATION_QUEUE = "$TestQueue"


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

'''
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


class DebugTestCase(unittest.TestCase):
    def setUp(self):
        self.tr = proto_helpers.StringTransport()
        self.tr.socket = MagicMock()
        self.connection_info = ConnectionInfo()
        self.reactor = Clock()
        self.reactor.connectTCP = MagicMock()
        self.factory = AMQPFactory(self.connection_info,
                                   queueschema,
                                   reactor=self.reactor)
    def test_debug(self):
        print(dir(self.factory))
'''


class AMQPFactoryTestCase(unittest.TestCase):

    def setUp(self):
        self._build_factory()
        self._build_protocol()
        self.reactor.advance(2)

    def _build_factory(self):
        self.tr = proto_helpers.StringTransport()
        self.tr.socket = MagicMock()

        self.connection_info = ConnectionInfo()
        self.reactor = Clock()
        self.reactor.connectTCP = MagicMock()
        self.factory = AMQPFactory(self.connection_info,
                                   queueschema,
                                   reactor=self.reactor)
        self.mock_connector = self.factory.connector

    def _build_protocol(self):
        self.proto = self.factory.buildProtocol(('127.0.0.1', 0, self.reactor))
        self.tr.proto = self.proto
        self.proto.makeConnection(self.tr)

    def tearDown(self):
        self.factory.shutdown()

    def test_buildProtocol_setup(self):
        '''After setUp, ensure that the protocol for our test factory
        is the same as the protocol we built.
        '''
        self.assertEqual(
            self.proto.__class__.__name__,
            self.factory.protocol.__name__
        )

    def test_default_errback(self):
        '''Create a deferred with a callback that will raise an error
        and test that it raises the expected RuntimeError type
        '''
        d = defer.Deferred()
        d.addCallback(BOMB)
        d.addErrback(self.factory._defaultErrback)

        test = self.assertFailure(d, RuntimeError)
        test.callback("test_default_errback")
        return test

    def _test_factory_hook(self, hook_name):
        '''Convenience method for testing factory hooks and triggers
        '''
        trigger_name = hook_name[1:]
        hook = getattr(self.factory, hook_name)
        trigger = getattr(self.factory, trigger_name)
        self.success = False

        @defer.inlineCallbacks
        def wait_for_hook(self):
            # wait for factory.onAuthenticated to be triggered
            yield hook
            self.success = True

        waiter = defer.Deferred().addCallback(wait_for_hook)
        waiter.callback(self)
        # let the reactor run, and confirm that the trigger is still waiting
        self.reactor.advance(2)
        self.assertFalse(self.success)
        # Trigger the Hook on the factory
        trigger('go')
        self.reactor.advance(2)
        self.assertTrue(self.success)

    def test_onConnectionMade_hook(self):
        self._test_factory_hook('_onConnectionMade')

    def test_onConnectionLost_hook(self):
        self._test_factory_hook('_onConnectionLost')

    def test_onConnectionFailed_hook(self):
        self._test_factory_hook('_onConnectionFailed')

    def test_onAuthenticated_hook(self):
        self._test_factory_hook('_onAuthenticated')

    def test_onInitialSend_hook(self):
        self._test_factory_hook('_onInitialSend')

    @patch('zenoss.protocols.twisted.amqp.AMQPFactory.resetDelay',
           autospec=True)
    def test_buildProtocol(self, m_reset_delay):
        '''ensure that factory.buildProtocol() returns an AMQProtocol object
        '''
        p = self.factory.buildProtocol(('127.0.0.1', 0, self.reactor))
        self.assertEqual(p.__class__.__name__, 'AMQProtocol')
        self.assertEqual(p.factory, self.factory)
        self.assertEqual(p.prefetch, self.factory.prefetch)
        # ensure creating a new protocol resets the reconnecting delay
        self.assertEqual(m_reset_delay.call_count, 1)

    def test_setPrefetch(self):
        '''ensure setPrefetch sets the factory's prefetch attribute
        '''
        tracer = object()
        self.factory.setPrefetch(tracer)
        self.assertEqual(tracer, self.factory.prefetch)

    def test_listen(self):
        try:
            backup = self.proto.listen_to_queue
            self.proto.listen_to_queue = MagicMock()

            queue = object()
            def process_message(self): pass
            callback = process_message

            self.assertTrue(self.factory.p is not None)
            self.factory.listen(queue, callback, exclusive=False)
            args = (queue, callback, False)
            # ensure factory.queues contains tuple of args
            self.assertIn(args, self.factory.queues)

            # ensure protocol.listen_to_queue was called
            self.proto.listen_to_queue.assert_called_with(*args)
        finally:
            self.proto.listen_to_queue = backup

    def test_createQueue(self):
        # factory.createQueue calls protocol create_queue
        #self.tr.proto.start = MagicMock(spec=self.proto.start)
        #chan = object()
        #self.proto.get_channel = MagicMock(spec=self.proto.get_channel,
        #                                   return_value=chan)
        queueIdentifier = INVALIDATION_QUEUE
        replacements = None
        #queue = object()
        queue = self.factory.queueSchema.getQueue(queueIdentifier,
                                                  replacements)

        m_proto_create_queue = MagicMock(spec=self.proto.create_queue)
        self.proto.create_queue = m_proto_create_queue
        d = self.factory.createQueue(INVALIDATION_QUEUE, replacements=None)
        self.reactor.advance(2)
        self.assertEqual(m_proto_create_queue.call_count, 1,
                         "protocol.create_queue was not called")
        return d




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

    @patch('zenoss.protocols.twisted.amqp.getAdapter', autospec=True)
    def test_create_queue(self, m_get_adapter):
        self.tr.proto.start = MagicMock(spec=self.proto.start)
        chan = object()
        self.proto.get_channel = MagicMock(spec=self.proto.get_channel,
                                           return_value=chan)
        cm = self.proto.connectionMade()
        self.reactor.advance(2)
        self.assertTrue(hasattr(self.proto, 'chan'))
        self.assertEqual(chan, self.proto.chan)

        d = self.proto.create_queue(INVALIDATION_QUEUE)
        self.reactor.advance(2)
        m_get_adapter.assert_called_with(self.proto.chan, IAMQPChannelAdapter)
        return d

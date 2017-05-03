from zenoss.protocols.twisted import amqp
from zenoss.protocols.twisted.amqp import AMQPFactory, IAMQPChannelAdapter

from zope.interface import implements
from zenoss.protocols.interfaces import IAMQPConnectionInfo, IQueueSchema
from zenoss.protocols.queueschema import Schema
from fixtures import queueschema
from txamqp.client import Closed

from mock import MagicMock
from mock import patch

from twisted.internet import defer, reactor, base
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
'''

def test_errback(din):
    print('test_errback caught exception')
    din.printTraceback()
    return din

'''
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
        d.callback('test_default_errback')
        self.reactor.advance(3)
        # default errback currently swallows the error
        # and returns a success deferred, with value None
        self.assertEqual(None, self.successResultOf(d))

    def _test_factory_hook(self, hook_name):
        '''Convenience method for testing factory hooks and triggers
        '''
        trigger_name = hook_name[1:]
        hook = getattr(self.factory, hook_name)
        trigger = getattr(self.factory, trigger_name)
        self.success = False

        @defer.inlineCallbacks
        def wait_for_hook(self):
            # wait for factory.<hook_name> to be triggered
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
        NOTE: not asynchronous
        '''
        p = self.factory.buildProtocol(('127.0.0.1', 0, self.reactor))
        self.assertEqual(p.__class__.__name__, 'AMQProtocol')
        self.assertEqual(p.factory, self.factory)
        self.assertEqual(p.prefetch, self.factory.prefetch)
        # ensure creating a new protocol resets the reconnecting delay
        self.assertEqual(m_reset_delay.call_count, 1)

    def test_setPrefetch(self):
        '''ensure setPrefetch sets the factory's prefetch attribute
        NOTE: not asynchronous
        '''
        tracer = object()
        self.factory.setPrefetch(tracer)
        self.assertEqual(tracer, self.factory.prefetch)

    def test_listen(self):
        '''Mock the protocol.listen_to_queue method directly,
        patching self.proto did not work when I tried it.
        '''
        backup = self.proto.listen_to_queue
        try:
            self.proto.listen_to_queue = MagicMock()
            queue = object()
            def process_message(self): pass
            callback = process_message
            self.assertTrue(self.factory.p is not None)

            d = self.factory.listen(queue, callback, exclusive=False)
            args = (queue, callback, False)
            self.reactor.advance(3)
            # we would use this if this method returned a deferred, it currently returns None
            #self.assertEqual('success', self.successResultOf(d))
            # ensure factory.queues contains tuple of args
            self.assertIn(args, self.factory.queues)
            # ensure protocol.listen_to_queue was called
            self.proto.listen_to_queue.assert_called_with(*args)
        finally:
            self.proto.listen_to_queue = backup

    @patch('zenoss.protocols.twisted.amqp.AMQProtocol.send',
           autospec=True)
    def test_send(self, m_protocol_send):
        '''appends parameters to factory message queue.
        returns a deffered, either protocol.send if protocol is set,
        or factory._onInitialSend if not
        '''
        exchangeIdentifier = "test_exchangeIdentifier"
        routing_key = "test_routing_key"
        message = "test_message"
        mandatory = None
        headers = True
        declareExchange = False
        args = [exchangeIdentifier,
                routing_key,
                message]
        kwargs = {'mandatory': mandatory,
                  'headers': headers,
                  'declareExchange': declareExchange}
        values = tuple(args + [mandatory, headers, declareExchange])

        self.assertEqual(m_protocol_send.call_count, 0)
        d = self.factory.send(*args, **kwargs)

        self.assertIn(tuple(values), self.factory.messages)
        self.assertEqual(m_protocol_send.call_count, 1)
        self.assertEqual(m_protocol_send.return_value, d)

    def test_send_without_protocol(self):
        '''if factory.p(rotocol) is None, return the _onInitialSend hook
        '''
        self.factory.p = None

        exchangeIdentifier = "test_exchangeIdentifier"
        routing_key = "test_routing_key"
        message = "test_message"
        mandatory = None
        headers = True
        declareExchange = False
        args = [exchangeIdentifier,
                routing_key,
                message]
        kwargs = {'mandatory': mandatory,
                  'headers': headers,
                  'declareExchange': declareExchange}
        values = tuple(args + [mandatory, headers, declareExchange])

        d_onInitialSend = self.factory._onInitialSend
        d = self.factory.send(*args, **kwargs)
        d.callback('test_send_without_protocol')
        self.reactor.advance(3)
        self.assertEqual('test_send_without_protocol', self.successResultOf(d))

        self.assertIn(tuple(values), self.factory.messages)
        self.assertEqual(d_onInitialSend, d)


    @patch('zenoss.protocols.twisted.amqp.AMQProtocol.acknowledge',
           autospec=True)
    def test_acknowledge(self, m_protocol_acknowledge):
        tracer = object()
        #test = defer.Deferred()
        d = self.factory.acknowledge(tracer)
        self.reactor.advance(4)
        m_protocol_acknowledge.assert_called_with(self.factory.p, tracer)
        self.assertEqual('success', self.successResultOf(d))


    @patch('zenoss.protocols.twisted.amqp.AMQProtocol.acknowledge',
           autospec=True,
           side_effect=Closed)
    def test_acknowledge_err(self, m_protocol_acknowledge):
        tracer = object()
        test = defer.Deferred()
        test.addCallback(self.factory.acknowledge).callback(tracer)
        self.reactor.advance(2)
        self.assertFailure(test, Closed)
        test.addCallback(
            m_protocol_acknowledge.assert_called_with(self.factory.p, tracer)
        )
        return test


    @patch('zenoss.protocols.twisted.amqp.AMQProtocol.reject',
           autospec=True)
    def test_reject(self, m_protocol_reject):
        message = object()
        requeue = object()
        test = defer.Deferred()
        test.addCallback(
            self.factory.reject
        ).callback((message, requeue))
        test.addCallback(
            m_protocol_reject.assert_called_with(self.factory.p, message, requeue)
        )
        return test
    # test_disconnect
    # test_shutdown

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

    # test_clientConnectionFailed
    # test_clientConnectionLost

    #def test_double_ack_error(self):


class AMQPProtocolTestCase(unittest.TestCase):

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

    def _connect(self):
        self.tr.proto.start = MagicMock(spec=self.proto.start)
        chan = object()
        self.proto.get_channel = MagicMock(spec=self.proto.get_channel,
                                           return_value=chan)
        cm = self.proto.connectionMade()
        return cm, chan

    def tearDown(self):
        self.factory.shutdown()

    def test_connectionMade(self):
        self.tr.proto.start = MagicMock(spec=self.proto.start)
        chan = object()
        self.proto.get_channel = MagicMock(spec=self.proto.get_channel,
                                           return_value=chan)

        d = self.proto.connectionMade()

        self.reactor.advance(3)

        self.assertTrue(self.proto._connected)
        self.assertTrue(chan is self.proto.chan)
        self.assertTrue(self.tr.proto.get_channel.call_count == 1)
        self.assertEqual('success', self.successResultOf(d))

    def test_connectionMade_authentication_errors(self):
        self.tr.proto.start = MagicMock(spec=self.proto.start)
        chan = object()
        self.proto.get_channel = MagicMock(spec=self.proto.get_channel,
                                           return_value=chan)

        # raise a txamqp.client.Closed exception when we call proto.start
        self.tr.proto.start.side_effect = Closed

        d = self.proto.connectionMade()
        self.reactor.advance(4)
        # ensure that the Closed error is raised
        #self.assertFailure(cm, Closed)
        self.failureResultOf(d).trap(Closed)

    @patch('zenoss.protocols.twisted.amqp.ReconnectingClientFactory.clientConnectionFailed',
           autospec=True)
    def test_connectionMade_handles_errors(self, m_ccf):
        self.tr.proto.start = MagicMock(spec=self.proto.start)
        chan = object()
        self.proto.get_channel = MagicMock(spec=self.proto.get_channel,
                                           return_value=chan)

        self.tr.proto.start.side_effect = BOMB

        d = self.proto.connectionMade()
        self.reactor.advance(4)
        #self.assertFailure(d, RuntimeError)
        self.failureResultOf(d).trap(RuntimeError)

    def test_get_channel_err(self):
        '''ensure get_channel raises a deferred Failure if an exception occurs
        '''
        self.proto.channel = MagicMock(spec=self.proto.channel,
                                       side_effect=RuntimeError)

        d = self.proto.get_channel()
        self.reactor.advance(3)
        #self.assertFailure(d, RuntimeError)
        self.failureResultOf(d).trap(RuntimeError)

    def test_listen_to_queue_err(self):
        '''ensure listen_to_queue raises errors properly
        '''
        #base.DelayedCall.debug = True

        test, chan = self._connect()
        #test = defer.Deferred()
        self.proto.get_queue = MagicMock(spec=self.proto.get_queue,
                                         side_effect=RuntimeError('hello'))
        queue = self.factory.queueSchema.getQueue(INVALIDATION_QUEUE)
        print(queue)
        self.proto._connected = False
        def callback_function(_): pass

        d = self.proto.listen_to_queue(queue, callback_function)
        self.reactor.advance(4)
        self.failureResultOf(d).trap(RuntimeError)

    def test_begin_listening_err(self):
        pass

    def test_get_queue_err(self):
        pass

    @patch('zenoss.protocols.twisted.amqp.getAdapter', autospec=True)
    def test_create_queue(self, m_get_adapter):
        '''self.tr.proto.start = MagicMock(spec=self.proto.start)
        chan = object()
        self.proto.get_channel = MagicMock(spec=self.proto.get_channel,
                                           return_value=chan)
        cm = self.proto.connectionMade()
        '''
        cm, chan = self._connect()
        self.reactor.advance(2)
        self.assertTrue(hasattr(self.proto, 'chan'))
        self.assertEqual(chan, self.proto.chan)

        d = self.proto.create_queue(INVALIDATION_QUEUE)
        self.reactor.advance(2)
        m_get_adapter.assert_called_with(self.proto.chan, IAMQPChannelAdapter)
        self.assertEqual('success', self.successResultOf(d))

    def test_send_message(self):
        #raise NotImplementedError
        pass

    def test_send(self):
        #raise NotImplementedError
        pass

    def test_acknowledge(self):
        #raise NotImplementedError
        pass

    def test_processMessages(self):
        #raise NotImplementedError
        pass

    def test_doCallback(self):
        #raise NotImplementedError
        pass

    def test_connectionLost(self):
        #raise NotImplementedError
        pass

    class message(object):
        def __init__(self):
            self.delivery_tag = 'tag'

    def test_acknowledge(self):
        '''ensure protocol.acknowledge returns a deferred
        '''
        #raise NotImplementedError
        #test, chan = self._connect()
        self.proto.chan = MagicMock(spec=IAMQPChannelAdapter)
        self.proto.chan.basic_ack = MagicMock()#side_effect=Closed)

        dout = self.proto.acknowledge(self.message())
        self.assertTrue(
            isinstance(dout, defer.Deferred)
        )
        return dout

    def test_acknowledge_err(self):
        self.proto.chan = MagicMock(spec=IAMQPChannelAdapter)
        self.proto.chan.basic_ack = MagicMock(side_effect=Closed)
        test = defer.Deferred()

        test.addCallback(self.proto.acknowledge)
        test.callback(self.message())
        self.reactor.advance(2)
        self.assertFailure(test, Closed)
        return test


    def test_reject(self):
        #raise NotImplementedError
        pass

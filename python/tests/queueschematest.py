###########################################################################
#
# This program is part of Zenoss Core, an open source monitoring platform.
# Copyright (C) 2010, Zenoss Inc.
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License version 2 or (at your
# option) any later version as published by the Free Software Foundation.
#
# For complete information please visit: http://www.zenoss.com/oss/
#
###########################################################################

import pkg_resources # Import this so zenoss.protocols will be found
import unittest
from fixtures import queueschema, protobuf
from zenoss.protocols.queueschema import MissingReplacementException

class TestQueueConfig(unittest.TestCase):

    def setUp(self):
        """
        Sets up the queue config
        """
        self.queueConfig = queueschema

    def tearDown(self):
        self.queueConfig = None

    def testGetExchange(self):
        config = self.queueConfig.getExchange('$TestExchange')
        self.assertEqual(config.name, 'zenoss.test')
        self.assertEqual(config.type, 'direct')
        self.assertEqual(config.durable, True)
        self.assertEqual(config.auto_delete, False)
        self.assertEqual(config.description, "Test exchange")
        self.assertEqual(config.arguments, {"exchange_arg1": "val1","exchange_arg2":False,"exchange_arg3":100})

    def testGetQueue(self):
        config = self.queueConfig.getQueue('$TestQueue')
        self.assertEqual(config.name, 'zenoss.queues.test')
        self.assertEqual(config.durable, True)
        self.assertEqual(config.exclusive, False)
        self.assertEqual(config.auto_delete, False)
        self.assertEqual(config.arguments, {"queue_arg1": "val1","queue_arg2": False,"queue_arg3": 1})

        binding = config.getBinding('$TestExchange')
        self.assertEqual('zenoss.test', binding.exchange.name)
        self.assertEqual('zenoss.test', binding.routing_key)
        self.assertEqual(binding.arguments, {"binding_arg1": "binding_val1","binding_arg2": False,"binding_arg3": 100})

        exchange = binding.exchange
        self.assertEqual(exchange.name, 'zenoss.test')

    def testHydrateProtobuf(self):
        content = protobuf.SerializeToString()
        name = protobuf.DESCRIPTOR.full_name
        testProto = self.queueConfig.hydrateProtobuf(name, content)
        self.assertEqual(testProto.uuid, protobuf.uuid)
        self.assertEqual(testProto.created_time, protobuf.created_time)

    def _compareReplacementExchange(self, exchange, replacements):
        self.assertEqual('zenoss.exchanges.{exchange_uuid}'.format(**replacements), exchange.name)
        self.assertEqual(exchange.type, 'topic')
        self.assertEqual(exchange.durable, True)
        self.assertEqual(exchange.auto_delete, False)
        self.assertEqual(exchange.description, 'Sample replacement exchange.')
        self.assertEqual(0, len(exchange.content_types))
        self.assertEqual(1, len(exchange.arguments))
        self.assertEqual('my argument {exchange_value}'.format(**replacements),
                         exchange.arguments['arg_{exchange_name}'.format(**replacements)])

    def testExchangeReplacements(self):
        from uuid import uuid4
        replacements = {
            'exchange_uuid': str(uuid4()),
            'exchange_name': 'replacement name',
            'exchange_value': 'replacement value',
        }

        # Verify that we get an exception if replacements aren't specified
        self.assertRaises(MissingReplacementException, self.queueConfig.getExchange, '$ReplacementExchange')

        exchange = self.queueConfig.getExchange('$ReplacementExchange', replacements)
        self._compareReplacementExchange(exchange, replacements)

    def testQueueReplacements(self):
        from uuid import uuid4
        replacement_names = ['queue_uuid','arg5','arg6','queue_name','arg7','arg8','key','name','arg1','arg2',
                             'exchange_uuid','exchange_name','exchange_value']
        replacements = dict((k,str(uuid4())) for k in replacement_names)

        # Verify we get an exception if replacements aren't specified
        self.assertRaises(MissingReplacementException, self.queueConfig.getQueue, '$ReplacementQueue')

        config = self.queueConfig.getQueue('$ReplacementQueue', replacements)
        self.assertEqual('zenoss.queues.{queue_uuid}'.format(**replacements), config.name)
        self.assertEqual(True, config.durable)
        self.assertEqual(False, config.exclusive)
        self.assertEqual(False, config.auto_delete)
        self.assertEqual('Replacement queue exchange.', config.description)
        queue_arguments = {
            'arg1': 'my {arg5} and {arg6}'.format(**replacements),
            'queue_arg_{queue_name}'.format(**replacements): 'my {arg7} and {arg8}'.format(**replacements),
        }
        self.assertEqual(queue_arguments, config.arguments)
        self.assertEquals(1, len(config.bindings))
        binding = config.bindings['$ReplacementExchange']
        self.assertEqual('zenoss.events.{key}'.format(**replacements), binding.routing_key)
        binding_arguments = {
            'binding_arg{name}'.format(**replacements): 'my binding argument {arg1} and {arg2}'.format(**replacements),
        }
        self.assertEqual(binding_arguments, binding.arguments)
        self._compareReplacementExchange(binding.exchange, replacements)

def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestQueueConfig))
    return suite

if __name__ == '__main__':
    unittest.main()


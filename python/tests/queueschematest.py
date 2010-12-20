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

import pkg_resources # Import this so zenoss.protocols will be found
import unittest
from fixtures import queueschema, protobuf

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

    def testGetQueue(self):
        config = self.queueConfig.getQueue('$TestQueue')
        self.assertEqual(config.name, 'zenoss.queues.test')
        exchange = config.getBinding("$TestExchange").exchange
        self.assertEqual(exchange.name, 'zenoss.test')

    def testHydrateProtobuf(self):
        content = protobuf.SerializeToString()
        name = protobuf.DESCRIPTOR.full_name
        testProto = self.queueConfig.hydrateProtobuf(name, content)
        self.assertEqual(testProto.uuid, protobuf.uuid)
        self.assertEqual(testProto.created_time, protobuf.created_time)

def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestQueueConfig))
    return suite

if __name__ == '__main__':
    unittest.main()


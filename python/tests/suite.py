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

from unittest import TestSuite, makeSuite
from queueschematest import TestQueueConfig
from servicetest import ProtobufRestServiceClientTest, SerializerTest
from triggerservicetest import TriggerServiceTest
from zepservicetest import ZepServiceTest

test_all = TestSuite()
test_classes = [
    TestQueueConfig,
    ProtobufRestServiceClientTest,
    SerializerTest,
    TriggerServiceTest,
    # TODO: Woefully out of date
#    ZepServiceTest,
]
for test_class in test_classes:
    test_all.addTest(makeSuite(test_class))


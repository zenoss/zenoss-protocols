##############################################################################
# 
# Copyright (C) Zenoss, Inc. 2011, all rights reserved.
# 
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
# 
##############################################################################


from unittest import TestSuite, makeSuite
from queueschematest import TestQueueConfig
from servicetest import ProtobufRestServiceClientTest, SerializerTest
from triggerservicetest import TriggerServiceTest
from zepservicetest import ZepServiceTest
from jsontest import JsonTest

test_all = TestSuite()
test_classes = [
    TestQueueConfig,
    ProtobufRestServiceClientTest,
    SerializerTest,
    TriggerServiceTest,
    JsonTest,
    # TODO: Woefully out of date
#    ZepServiceTest,
]
for test_class in test_classes:
    test_all.addTest(makeSuite(test_class))

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

from protobufs.test_pb2 import TestMessage, EmptyTestMessage, OPTION_A, OPTION_B, OPTION_C
from uuid import uuid4
from time import time
from zenoss.protocols.queueschema import Schema

protobuf_data = {
    'uuid' : str(uuid4()),
    'created_time' : int(time() * 1000),
    'test_enum' : OPTION_B,
    'messages' : [
        {
            'uuid' : str(uuid4()),
            'test_enum' : OPTION_A,
        },
        {
            'uuid' : str(uuid4()),
            'test_enum' : OPTION_C,
        },
    ],
    'nested' : {
        'uuid' : str(uuid4()),
        'test_enum' : OPTION_A,
    }
}

protobuf = TestMessage()
protobuf.uuid = protobuf_data['uuid']
protobuf.created_time = protobuf_data['created_time']
protobuf.test_enum = protobuf_data['test_enum']
protobuf.nested.uuid = protobuf_data['nested']['uuid']
protobuf.nested.test_enum = protobuf_data['nested']['test_enum']

_m1 = protobuf.messages.add()
_m1.uuid = protobuf_data['messages'][0]['uuid']
_m1.test_enum = protobuf_data['messages'][0]['test_enum']

_m2 = protobuf.messages.add()
_m2.uuid = protobuf_data['messages'][1]['uuid']
_m2.test_enum = protobuf_data['messages'][1]['test_enum']

empty_protobuf = EmptyTestMessage()

class MockSchema:
    content_types = {
        "$TestMessage" : {
            "java_class" : "org.zenoss.test.TestMessage",
            "python_class" : "tests.protobufs.test_pb2.TestMessage",
            "content_type" : "application/x-protobuf",
            "x-protobuf" : "org.zenoss.test.TestMessage"
        },
        "$EmptyTestMessage" : {
            "java_class" : "org.zenoss.test.EmptyTestMessage",
            "python_class" : "tests.protobufs.test_pb2.EmptyTestMessage",
            "content_type" : "application/x-protobuf",
            "x-protobuf" : "org.zenoss.test.EmptyTestMessage"
        }
    }
    exchanges = {
        "$TestExchange" : {
            "name" : "zenoss.test",
            "type" : "direct",
            "durable" : True,
            "auto_delete" : False,
            "description" : "Test exchange",
            "content_types" : ["$TestMessage"],
            "routing_key_regexp" : "zenoss.test"
        }
    }
    queues = {
        "$TestQueue" : {
            "name" : "zenoss.queues.test",
            "durable" : True,
            "exclusive" : False,
            "auto_delete" : False,
            "description" : "Test queue",
            "bindings" : [
                {
                    "exchange" : "$TestExchange",
                    "routing_key" : "zenoss.test"
                }
            ]
        }

    }

queueschema = Schema(MockSchema)
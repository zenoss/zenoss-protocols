##############################################################################
# 
# Copyright (C) Zenoss, Inc. 2010, all rights reserved.
# 
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
# 
##############################################################################


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

mock_schema = """\
{
    "content_types": {
        "$TestMessage": {
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
    },
    "exchanges": {
        "$TestExchange" : {
            "name" : "zenoss.test",
            "type" : "direct",
            "durable" : true,
            "auto_delete" : false,
            "description" : "Test exchange",
            "content_types" : ["$TestMessage"],
            "arguments": {
                "exchange_arg1": {
                    "value": "val1"
                },
                "exchange_arg2": {
                    "value": false
                },
                "exchange_arg3": {
                    "value": 100
                }
            }
        },
        "$ReplacementExchange": {
            "name" : "zenoss.exchanges.{exchange_uuid}",
            "type" : "topic",
            "durable" : true,
            "auto_delete" : false,
            "description" : "Sample replacement exchange.",
            "content_types" : [],
            "arguments": {
                "arg_{exchange_name}": {
                    "value": "my argument {exchange_value}"
                }
            }
        },
        "$ExplicitPropertiesExchange": {
            "name": "zenoss.exchanges.explicit",
            "type": "topic",
            "durable" : true,
            "auto_delete" : false,
            "description" : "Sample properties exchange.",
            "content_types" : []
        },
        "$DefaultPropertiesExchange": {
            "name": "zenoss.exchanges.default",
            "type": "topic",
            "durable" : true,
            "auto_delete" : false,
            "description" : "Sample properties exchange.",
            "content_types" : []
        }
    },
    "queues": {
        "$TestQueue" : {
            "name" : "zenoss.queues.test",
            "durable" : true,
            "exclusive" : false,
            "auto_delete" : false,
            "description" : "Test queue",
            "arguments": {
                "queue_arg1": {
                    "value": "val1"
                },
                "queue_arg2": {
                    "value": false
                },
                "queue_arg3": {
                    "value": 1
                }
            },
            "bindings" : [
                {
                    "exchange" : "$TestExchange",
                    "routing_key" : "zenoss.test",
                    "arguments": {
                        "binding_arg1": {
                            "value": "binding_val1"
                        },
                        "binding_arg2": {
                            "value": false
                        },
                        "binding_arg3": {
                            "value": 100
                        }
                    }
                }
            ]
        },
        "$ReplacementQueue": {
            "name" : "zenoss.queues.{queue_uuid}",
            "durable" : true,
            "exclusive" : false,
            "auto_delete" : false,
            "description" : "Replacement queue exchange.",
            "arguments": {
                "arg1": {
                    "value": "my {arg5} and {arg6}"
                },
                "queue_arg_{queue_name}": {
                    "value": "my {arg7} and {arg8}"
                }
            },
            "bindings" : [
                {
                    "exchange" : "$ReplacementExchange",
                    "routing_key" : "zenoss.events.{key}",
                    "arguments": {
                        "binding_arg{name}": {
                            "value": "my binding argument {arg1} and {arg2}"
                        }
                    }
                }
            ]
        },
        "$ExplicitPropertiesQueue": {
            "name" : "zenoss.queues.properties",
            "durable" : true,
            "exclusive" : false,
            "auto_delete" : false,
            "description" : "Propertied queue."
        },
        "$DefaultPropertiesQueue": {
            "name" : "zenoss.queues.properties",
            "durable" : true,
            "exclusive" : false,
            "auto_delete" : false,
            "description" : "Propertied queue."
        }
    }
}
"""

explicit_properties = {
    'exchange.$ExplicitPropertiesExchange.delivery_mode': '1',
    'queue.$ExplicitPropertiesQueue.x-message-ttl': '54321',
    'queue.$ExplicitPropertiesQueue.x-expires': '11235'
}

default_properties = {
    'exchange.default.delivery_mode': '1',
    'exchange.$ExplicitPropertiesExchange.delivery_mode': '2',
    'queue.default.x-message-ttl':'54321',
    'queue.default.x-expires':'11235',
    'queue.$ExplicitPropertiesQueue.x-message-ttl':'12345',
    'queue.$ExplicitPropertiesQueue.x-expires':'81321'
}

from json import loads
queueschema = Schema(loads(mock_schema))


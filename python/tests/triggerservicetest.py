##############################################################################
# 
# Copyright (C) Zenoss, Inc. 2010, all rights reserved.
# 
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
# 
##############################################################################


import unittest
import pkg_resources # Import this so zenoss.protocols will be found
from zenoss.protocols.services.triggers import TriggerServiceClient
from zenoss.protocols.jsonformat import from_dict
from zenoss.protocols.queueschema import Schema
from zenoss.protocols.data.queueschema import SCHEMA
from zenoss.protocols.protobufs.zep_pb2 import EventTrigger, EventTriggerSet, EventTriggerSubscriptionSet, RULE_TYPE_JYTHON
import BaseHTTPServer
import threading
from uuid import uuid4
import logging
from time import time

test_trigger_data = dict(
    uuid = str(uuid4()),
    name = 'test trigger',
    enabled = True,
    send_clear = False,
    rule = dict(
        api_version = 1,
        type = RULE_TYPE_JYTHON,
        source = ''
    )
)
mock_trigger = from_dict(EventTrigger, test_trigger_data)

mock_trigger_set = from_dict(EventTriggerSet, dict(
    triggers = [
        test_trigger_data
    ]
))

# for testing subscription updates.
mock_subscriber_uuid = str(uuid4())

mock_trigger_subscription = dict(
    delay_seconds = 10,
    repeat_seconds = 60,
    subscriber_uuid = mock_subscriber_uuid,
    trigger_uuid = test_trigger_data['uuid'],
)

mock_subscription_set = from_dict(EventTriggerSubscriptionSet, dict(
    subscriptions = [
        mock_trigger_subscription
    ]

))

class MockTriggerServiceHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    def do_GET(self):
        # getting all triggers
        if self.path == TriggerServiceClient._base_uri:
            self.send_response(200)
            self.send_header('Content-Type', 'application/x-protobuf')
            self.send_header('X-Protobuf-FullName', mock_trigger_set.DESCRIPTOR.full_name)
            self.end_headers()
            self.wfile.write(mock_trigger_set.SerializeToString())
        # getting a single trigger
        elif self.path == TriggerServiceClient._base_uri + mock_trigger.uuid:
            self.send_response(200)
            self.send_header('Content-Type', 'application/x-protobuf')
            self.send_header('X-Protobuf-FullName', mock_trigger.DESCRIPTOR.full_name)
            self.end_headers()
            self.wfile.write(mock_trigger.SerializeToString())

    def do_POST(self):
        # create a single trigger
        if self.path == TriggerServiceClient._base_uri + mock_trigger.uuid:
            if self.rfile.read(int(self.headers['content-length'])) == mock_trigger.SerializeToString():
                self.send_response(201)
                self.end_headers()
            else:
                self.send_response(500)
                self.end_headers()

    def do_PUT(self):
        # update a single trigger
        if self.path == TriggerServiceClient._base_uri + mock_trigger.uuid:
            if self.rfile.read(int(self.headers['content-length'])) == mock_trigger.SerializeToString():
                self.send_response(204)
                self.end_headers()
            else:
                self.send_response(500)
                self.end_headers()

        # update trigger subscriptions
        if self.path == TriggerServiceClient._base_uri + 'subscriptions/' + mock_subscriber_uuid:
            if self.rfile.read(int(self.headers['content-length'])) == mock_subscription_set.SerializeToString():
                self.send_response(204)
                self.end_headers()
            else:
                self.send_response(500)
                self.end_headers()
        
    def do_DELETE(self):
        if self.path == TriggerServiceClient._base_uri + mock_trigger.uuid:
            self.send_response(204)
            self.end_headers()
        else:
            self.send_response(500)
            self.end_headers()
            
            
class TriggerServiceTest(unittest.TestCase):
    def setUp(self):
        # Start a mock server on a random port
        self.httpd = BaseHTTPServer.HTTPServer(('', 0), MockTriggerServiceHandler)
        self.port = self.httpd.socket.getsockname()[1]

        thread = threading.Thread(target=self.httpd.serve_forever)
        thread.daemon = True
        thread.start()

        self.schema = Schema(SCHEMA)

        # Connect the client to the random port
        self.client = TriggerServiceClient('http://localhost:%d' % self.port, self.schema)

    def test_get_triggers(self):
        response, content = self.client.getTriggers()
        assert response['content-type'] == 'application/x-protobuf'
        assert content.SerializeToString() == mock_trigger_set.SerializeToString()
        
    def test_add_trigger(self):
        response, content = self.client.addTrigger(mock_trigger)
        assert response.status == 204
    
    def test_remove_trigger(self):
        response, content = self.client.removeTrigger(mock_trigger.uuid)
        assert response.status == 204
    
    def test_get_trigger(self):
        response, content = self.client.getTrigger(mock_trigger.uuid)
        assert content.SerializeToString() == mock_trigger.SerializeToString()
        assert response.status == 200
    
    def test_update_trigger(self):
        response, content = self.client.updateTrigger(mock_trigger)
        assert response.status == 204
        
    def test_update_subscriptions(self):
        response, content = self.client.updateSubscriptions(mock_subscriber_uuid, mock_subscription_set)
        assert response.status == 204
        
    def tearDown(self):
        self.httpd.shutdown()


if __name__ == '__main__':
    unittest.main()

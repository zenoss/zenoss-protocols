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
from zenoss.protocols.data.queueschema import SCHEMA
from zenoss.protocols.queueschema import Schema
from zenoss.protocols.services.zep import ZepServiceClient
from zenoss.protocols.protobufs.zep_pb2 import EventSummary, EventSummaryResult, EventSummaryUpdate, STATUS_NEW
from zenoss.protocols.jsonformat import from_dict
import BaseHTTPServer
import threading
from uuid import uuid4
import logging
from time import time

log = logging.getLogger(__name__)

test_event_summary = from_dict(EventSummary, dict(
    uuid = str(uuid4()),
    status = STATUS_NEW,
    first_seen_time = int(time() * 1000),
    status_change_time = int(time() * 1000),
    last_seen_time = int(time() * 1000),
    count = 1,
    occurrence = [
        dict(
            uuid = str(uuid4()),
            created_time = int(time() * 1000),
        ),
    ]
))

test_event_summary2 = from_dict(EventSummary, dict(
    uuid = str(uuid4()),
    status = STATUS_NEW,
    first_seen_time = int(time() * 1000),
    status_change_time = int(time() * 1000),
    last_seen_time = int(time() * 1000),
    count = 1,
    occurrence = [
        dict(
            uuid = str(uuid4()),
            created_time = int(time() * 1000),
        ),
    ]
))

test_event_result = EventSummaryResult()
event = test_event_result.events.add()
event.MergeFrom(test_event_summary)
event = test_event_result.events.add()
event.MergeFrom(test_event_summary2)

test_event_summary_update = EventSummaryUpdate()
test_event_summary_update.status = STATUS_NEW

class MockZepServiceHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == ZepServiceClient._base_uri + test_event_summary.uuid:
            self.send_response(200)
            self.send_header('Content-Type', 'application/x-protobuf')
            self.send_header('X-Protobuf-FullName', test_event_summary.DESCRIPTOR.full_name)
            self.end_headers()
            self.wfile.write(test_event_summary.SerializeToString())
        elif self.path == ZepServiceClient._base_uri:
            self.send_response(200)
            self.send_header('Content-Type', 'application/x-protobuf')
            self.send_header('X-Protobuf-FullName', test_event_result.DESCRIPTOR.full_name)
            self.end_headers()
            self.wfile.write(test_event_result.SerializeToString())

    def do_POST(self):
        pass

    def do_PUT(self):
        if self.path == ZepServiceClient._base_uri + 'summary/%s' % test_event_summary.uuid:
            if self.rfile.read(int(self.headers['content-length'])) == test_event_summary_update.SerializeToString():
                self.send_response(204)
                self.end_headers()
            else:
                self.send_response(500)
                self.end_headers()

    def do_DELETE(self):
        self.send_response(404)
        self.end_headers()

class ZepServiceTest(unittest.TestCase):
    def setUp(self):
        # Start a mock server on a random port
        self.httpd = BaseHTTPServer.HTTPServer(('', 0), MockZepServiceHandler)
        self.port = self.httpd.socket.getsockname()[1]

        thread = threading.Thread(target=self.httpd.serve_forever)
        thread.daemon = True
        thread.start()

        # Connect the client to the random port
        schema = Schema(SCHEMA)
        self.client = ZepServiceClient('http://localhost:%d' % self.port, schema)

    def test_get_event_summary(self):
        response, content = self.client.getEventSummary(test_event_summary.uuid)
        assert response['content-type'] == 'application/x-protobuf'
        assert content.SerializeToString() == test_event_summary.SerializeToString()

    def test_get_event_summaries(self):
        response, content = self.client.getEventSummaries()
        assert response['content-type'] == 'application/x-protobuf'
        assert content.SerializeToString() == test_event_result.SerializeToString()
        
    def test_update_event_summary(self):
        response, content = self.client.updateEventSummary(test_event_summary.uuid, test_event_summary_update)
        assert response.status == 204

    def tearDown(self):
        self.httpd.shutdown()


if __name__ == '__main__':
    unittest.main()

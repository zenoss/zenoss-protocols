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

import unittest
import httplib2
import pkg_resources # Import this so zenoss.protocols will be found
from zenoss.protocols.services import ProtobufSerializer, ProtobufRestServiceClient
from fixtures import protobuf_data, protobuf, empty_protobuf, queueschema
import BaseHTTPServer
import threading

class TestServiceHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-Type', 'application/x-protobuf')
        self.send_header('X-Protobuf-FullName', protobuf.DESCRIPTOR.full_name)
        self.end_headers()
        self.wfile.write(protobuf.SerializeToString())

    def do_POST(self):
        self.send_response(204)
        self.end_headers()

    def do_PUT(self):
        self.send_response(204)
        self.end_headers()

    def do_DELETE(self):
        self.send_response(204)
        self.end_headers()

class SerializerTest(unittest.TestCase):
    def test_protobuf_serializer(self):
        serializer = ProtobufSerializer(queueschema)

        headers, content = serializer.dump({}, protobuf)

        assert headers['Content-Type'] == 'application/x-protobuf'
        assert headers['X-Protobuf-FullName'] == protobuf.DESCRIPTOR.full_name
        assert content == protobuf.SerializeToString()

    def test_protobuf_deserializer(self):
        serializer = ProtobufSerializer(queueschema)

        content = protobuf.SerializeToString()
        response = httplib2.Response({
            'status' : 200,
            'content-type' : 'application/x-protobuf',
            'x-protobuf-fullname' : protobuf.DESCRIPTOR.full_name,
        })

        response, content = serializer.load(response, content)
        assert content.SerializeToString() == protobuf.SerializeToString()

    def test_empty_string_serialization(self):
        serializer = ProtobufSerializer(queueschema)
        
        content = empty_protobuf.SerializeToString()
        assert content == ''
        
        response = httplib2.Response({
            'status' : 200,
            'content-type' : 'application/x-protobuf',
            'x-protobuf-fullname' : empty_protobuf.DESCRIPTOR.full_name,
        })
        
        response, content = serializer.load(response, content)
        assert content.SerializeToString() == empty_protobuf.SerializeToString()
        
        
        
class ProtobufRestServiceClientTest(unittest.TestCase):
    def setUp(self):
        self.httpd = BaseHTTPServer.HTTPServer(('', 0), TestServiceHandler)
        self.port = self.httpd.socket.getsockname()[1]

        thread = threading.Thread(target=self.httpd.serve_forever)
        thread.daemon = True
        thread.start()

        self.client = ProtobufRestServiceClient('http://localhost:%d' % self.port, schema=queueschema)

    def test_get_message(self):
        response, content = self.client.get('test_message')
        assert response['content-type'] == 'application/x-protobuf'
        assert content.SerializeToString() == protobuf.SerializeToString()

    def test_post_message(self):
        response, content = self.client.post('test_message', protobuf)
        assert response.status in (200, 204)
        assert content == ''

    def test_put_message(self):
        response, content = self.client.put('test_message', protobuf)
        assert response.status in (200, 204)
        assert content == ''

    def test_delete_message(self):
        response, content = self.client.delete('test_message')
        assert response.status in (200, 204)
        assert content == ''

    def tearDown(self):
        self.httpd.shutdown()


if __name__ == '__main__':
    unittest.main()
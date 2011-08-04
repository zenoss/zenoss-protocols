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

import json
import httplib2
import urlparse
import urllib
import socket
import errno
from zenoss.protocols import queueschema

class ServiceException(Exception):
    pass

class SerializeException(ServiceException):
    pass

class ServiceResponseError(ServiceException):
    def __init__(self, message, status, request, response, content):
        self.response = response
        self.request = request
        self.content = content
        self.status = status
        super(ServiceResponseError, self).__init__(message)

class ServiceConnectionError(ServiceException):
    def __init__(self, message, error):
        """
        @param str message The error message
        @param Exception error The originating error (usually a socket.error)
        """
        self.error = error
        super(ServiceConnectionError, self).__init__(message)

class RestSerializer(object):
    def dump(self, headers, body):
        return headers, body

    def load(self, response, content):
        return response, content

class UrlEncodedSerializer(RestSerializer):
    def dump(self, headers, body):
        if isinstance(body, dict):
            body = urllib.urlencode(body)
        return headers, body

class RestRequest(object):
    def __init__(self, uri, method='GET', headers=None, body=None):
        # Don't send unicode URLs - leads to UnicodeDecodeError in Python 2.7 httplib
        if isinstance(uri, unicode):
            uri = uri.encode('utf-8')
        self.uri = uri
        self.method = method
        self.headers = headers
        self.body = body

class RestServiceClient(object):
    _default_headers = {}
    _serializer = UrlEncodedSerializer()

    def __init__(self, uri, timeout=60, connection_error_class=ServiceConnectionError):
        self._connection_error_class = connection_error_class
        self._uri_parts = urlparse.urlsplit(uri, allow_fragments=False)
        self.default_params = urlparse.parse_qs(self._uri_parts.query)

        self.http = httplib2.Http(timeout=timeout)
        if self._uri_parts.username or self._uri_parts.password:
            self.http.add_credentials(self._uri_parts.username, self._uri_parts.password)

    def _cleanParams(self, params):
        """
        Remove Nones from any list paramaters and any parameters that are None.
        """
        newParams = {}
        for key, value in params.iteritems():
            if isinstance(value, list):
                values = filter(None, value)
                if len(values):
                    newParams[key] = values
            elif value is not None:
                newParams[key] = value

        return newParams

    def uri(self, path=None, params={}):
        if params:
            params = dict(self.default_params, **params)
        else:
            params = self.default_params

        query = urllib.urlencode(self._cleanParams(params), True)

        fullPath = self._uri_parts.path
        if path:
            fullPath = urlparse.urljoin(fullPath, path)

        return urlparse.urlunsplit((self._uri_parts.scheme, self._uri_parts.netloc, fullPath, query, ''))

    def _buildRequest(self, path, method='GET', params={}, headers={}, body=None):
        if self._default_headers:
            headers = dict(self._default_headers, **headers)
        return RestRequest(self.uri(path, params), method=method, headers=headers, body=body)

    def _executeRequest(self, request):
        try:
            response, content = self.http.request(request.uri, method=request.method, headers=request.headers, body=request.body)
        except socket.timeout as e:
            raise self._connection_error_class('Timed out connecting to service.', e)
        except socket.error as e:
            if e.errno == errno.ECONNREFUSED:
                raise self._connection_error_class('Could not connect to service.', e)
            else:
                raise

        if not (response.status >= 200 and response.status <= 299):
            raise ServiceResponseError(response.reason, response.status, request, response, content)

        return response, content

    def _request(self, path, method='GET', params={}, headers={}):
        request = self._buildRequest(path, method, params, headers)
        response, content = self._executeRequest(request)
        return self._serializer.load(response, content)

    def _request_with_body(self, path, body=None, method='POST', params={}, headers={}):
        request = self._buildRequest(path, method, params, headers, body=body)

        headers, body = self._serializer.dump(request.headers, request.body)
        request.headers = headers
        request.body = body

        response, content = self._executeRequest(request)

        return self._serializer.load(response, content)

    def post(self, path, body, params={}, headers={}):
        return self._request_with_body(path, method='POST', body=body, params=params, headers=headers)

    def put(self, path, body, params={}, headers={}):
        return self._request_with_body(path, method='PUT', body=body, params=params, headers=headers)

    def get(self, path, params={}, headers={}):
        return self._request(path, method='GET', params=params, headers=headers)

    def delete(self, path, params={}, headers={}):
        return self._request(path, method='DELETE', params=params, headers=headers)


class ProtobufSerializer(RestSerializer):
    _content_type = 'application/x-protobuf'
    _protobuf_header = 'X-Protobuf-FullName'

    def __init__(self, protobuf_schema=None):
        self._protobuf_schema = protobuf_schema or queueschema

    def _get_headers(self, protobuf):
        return {
            'Content-Type' : self._content_type,
            self._protobuf_header : protobuf.DESCRIPTOR.full_name,
        }

    def dump(self, headers, body):
        headers = dict(headers, **self._get_headers(body))
        body = body.SerializeToString()

        return headers, body

    def load(self, response, content):
        if response.status == 200:
            if response['content-type'] != self._content_type:
                raise SerializeException('Expected "%s" got "%s"' % (self._content_type, response['content-type']))

            protobuf_type = response[self._protobuf_header.lower()]
            content = self._protobuf_schema.hydrateProtobuf(protobuf_type, content)

        return response, content


class ProtobufRestServiceClient(RestServiceClient):
    _default_headers = {
        'Accept' : ProtobufSerializer._content_type,
    }

    def __init__(self, uri, schema=None, connection_error_class=ServiceConnectionError, **kwargs):
        self._serializer = ProtobufSerializer(schema)
        super(ProtobufRestServiceClient, self).__init__(uri, connection_error_class=connection_error_class, **kwargs)


class JsonSerializer(RestSerializer):
    _content_type = 'application/json'

    def dump(self, headers, body):
        headers['content-type'] = self._content_type
        return headers, json.dumps(body)

    def load(self, response, content):
        if response.status == 200:
            content = json.loads(content)
        return response, content


class JsonRestServiceClient(RestServiceClient):

    def __init__(self, uri, schema=None, connection_error_class=ServiceConnectionError, **kwargs):
        self._serializer = JsonSerializer()
        super(JsonRestServiceClient, self).__init__(uri, connection_error_class=connection_error_class, **kwargs)

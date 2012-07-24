##############################################################################
# 
# Copyright (C) Zenoss, Inc. 2010-2012, all rights reserved.
# 
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
# 
##############################################################################


import json
import httplib2
import urlparse
import urllib
import socket
import errno
from urllib3 import connection_from_url
from urllib3.exceptions import TimeoutError, MaxRetryError
import logging
from time import time
from threading import RLock

log = logging.getLogger('zen.protocols.services')

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
        @param message The error message
        @param error The originating error (usually a socket.error)
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
    _pools = {}
    _rlock = RLock()

    def __init__(self, uri, timeout=60, connection_error_class=ServiceConnectionError):
        self._connection_error_class = connection_error_class
        self._uri_parts = urlparse.urlsplit(uri, allow_fragments=False)
        self.default_params = urlparse.parse_qs(self._uri_parts.query)
        self._default_timeout = timeout

        pool_key = (self._uri_parts.scheme, self._uri_parts.hostname, self._uri_parts.port)
        with RestServiceClient._rlock:
            self._pool = RestServiceClient._pools.get(pool_key)
            if not self._pool:
                log.debug("Creating new HTTP connection pool to: %s", uri)
                self._pool = connection_from_url(uri)
                RestServiceClient._pools[pool_key] = self._pool

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
            start_time = time()
            urllib3_response = self._pool.urlopen(request.method, request.uri, body=request.body,
                headers=request.headers, timeout=self._default_timeout, retries=0)

            # Convert response from urllib3 to httpLib2 for compatibility with existing callers
            response = httplib2.Response(urllib3_response.headers)
            response['status'] = str(urllib3_response.status)
            response.status = urllib3_response.status
            response.reason = urllib3_response.reason
            response.version = urllib3_response.version

            content = urllib3_response.data
            elapsed_time = time() - start_time
            log.debug("Elapsed time calling %s: %s", request.uri, elapsed_time)
        except (socket.timeout, TimeoutError) as e:
            raise self._connection_error_class('Timed out connecting to service.', e)
        except socket.error as e:
            if e.errno == errno.ECONNREFUSED or e.errno == errno.EINVAL:
                raise self._connection_error_class('Could not connect to service.', e)
            else:
                raise
        except MaxRetryError as e:
            # urllib3 raises this when it cannot establish a connection
            raise self._connection_error_class('Could not connect to service.', e)


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

    def __init__(self, protobuf_schema):
        super(RestSerializer, self).__init__()
        self._protobuf_schema = protobuf_schema

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

    def __init__(self, uri, queueSchema, connection_error_class=ServiceConnectionError, **kwargs):
        super(ProtobufRestServiceClient, self).__init__(uri, connection_error_class=connection_error_class, **kwargs)
        self._serializer = ProtobufSerializer(queueSchema)


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

    def __init__(self, uri, connection_error_class=ServiceConnectionError, **kwargs):
        super(JsonRestServiceClient, self).__init__(uri, connection_error_class=connection_error_class, **kwargs)
        self._serializer = JsonSerializer()

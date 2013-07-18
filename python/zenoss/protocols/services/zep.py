##############################################################################
# 
# Copyright (C) Zenoss, Inc. 2010, all rights reserved.
# 
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
# 
##############################################################################


import logging
from zenoss.protocols.services import ProtobufRestServiceClient, ServiceConnectionError
from zenoss.protocols.jsonformat import from_dict
from zenoss.protocols.protobufs.zep_pb2 import EventSummary, Event, EventNote, EventSummaryUpdate, EventSort, EventSummaryUpdateRequest, EventSummaryRequest, EventQuery, ZepConfig
from zenoss.protocols.protobufs.zep_pb2 import STATUS_NEW, STATUS_ACKNOWLEDGED, STATUS_CLOSED
from zenoss.protocols.protobufutil import ProtobufEnum, listify
from datetime import timedelta

log = logging.getLogger('zepclient')

EventSeverity = ProtobufEnum(Event, 'severity')
EventStatus = ProtobufEnum(EventSummary, 'status')
EventSortField = ProtobufEnum(EventSort, 'field')
EventSortDirection = ProtobufEnum(EventSort, 'direction')

ZERO = timedelta(0)
HOUR = timedelta(hours=1)

class ZepServiceException(Exception):
    pass

class ZepConnectionError(ServiceConnectionError):
    pass

class ZepServiceClient(object):
    _base_uri = '/zeneventserver/api/1.0/events/'
    _timeFormat = "%Y-%m-%dT%H:%M:%S.%fZ"

    def __init__(self, uri, queueSchema):
        self.client = ProtobufRestServiceClient(uri.rstrip('/') + self._base_uri, queueSchema,
                                                connection_error_class=ZepConnectionError)

    def getEventSummariesFromArchive(self, offset=0, limit=100, keys=None,
                                     sort=None, filter=None,
                                     exclusion_filter=None):
        request = self._buildRequest(offset, limit, sort, filter,
                                     exclusion_filter)
        return self.client.post('archive', body=request)

    def getEventSummaries(self, offset=0, limit=100, keys=None, sort=None,
                          filter=None, exclusion_filter=None):
        """
        Return a list of event summaries, optionally matching an EventFilter.

        @param filter EventFilter
        @param keys List of keys to return
        """
        request = self._buildRequest(offset, limit, sort, filter,
                                     exclusion_filter)
        return self.client.post('', body=request)

    def _buildRequest(self, offset, limit, sort, event_filter,
                      exclusion_filter):
        req = EventSummaryRequest()
        if event_filter is not None:
            req.event_filter.MergeFrom(event_filter)
        if exclusion_filter is not None:
            req.exclusion_filter.MergeFrom(exclusion_filter)
        for eventSort in filter(None, listify(sort)):
            sort = req.sort.add()
            sort.MergeFrom(eventSort)
        req.offset = offset
        req.limit = limit
        return req

    def addNote(self, uuid, message, userUuid=None, userName=None):
        """
        Add a note (what used to be called log) to an event summary.
        """

        note = from_dict(EventNote, dict(
            user_uuid = userUuid,
            user_name = userName,
            message = message
        ))

        return self.client.post('%s/notes' % uuid, body=note)


    def postNote(self, uuid, note):
        return self.client.post('{0}/notes'.format(uuid), body=note)


    def updateEventSummaries(self, update, event_filter=None, exclusion_filter=None, limit=None, timeout=None):
        """
        @param update: EventSummaryUpdate protobuf
        @param event_filter: EventFilter protobuf
        @param exclusion_filter: EventFilter protobuf
        @param limit: integer
        @param timeout: integer
        """
        search_uuid = self.createSavedSearch(event_filter=event_filter, exclusion_filter=exclusion_filter, timeout=timeout)
        updateRequest = EventSummaryUpdateRequest()
        updateRequest.event_query_uuid = search_uuid
        updateRequest.update_fields.MergeFrom(update)
        if limit:
            updateRequest.limit = limit

        log.debug('Update Request: %s', str(updateRequest))
        status, response = self.client.put('search/' + search_uuid, body=updateRequest)

        # If a limit is not specified, loop on the request until it has completed and return a summary
        # of all of the requests. This simplifies usage of the API.
        sum_updated = response.updated
        if not limit:
            while response.HasField('next_request'):
                status, response = self.nextEventSummaryUpdate(response.next_request)
                sum_updated += response.updated
            response.updated = sum_updated
        else:
            # Clean up saved search
            if not response.HasField('next_request'):
                self.deleteSavedSearch(search_uuid)

        return status, response

    def getEventSummary(self, uuid):
        """
        Get an event summary by event summary uuid.
        """
        return self.client.get('%s' % uuid)

    def nextEventSummaryUpdate(self, next_request):
        """
        Continues the next batch of event summary updates. This call uses the next_request
        field from the last update request to update the next range of events.

        @param next_request: EventSummaryUpdateResponse.next_request from previous response.
        """
        log.debug('Next Update Request: %s', str(next_request))
        status, response = self.client.put('search/' + next_request.event_query_uuid, body=next_request)
        if not response.HasField('next_request'):
            self.deleteSavedSearch(next_request.event_query_uuid)
        return status, response

    def closeEventSummaries(self, userUuid, userName=None, event_filter=None, exclusionFilter=None, limit=None,
                            timeout=None):
        update = from_dict(EventSummaryUpdate, dict(
            status = STATUS_CLOSED,
            current_user_uuid = userUuid,
            current_user_name = userName,
        ))
        return self.updateEventSummaries(update, event_filter=event_filter, exclusion_filter=exclusionFilter,
                                         limit=limit, timeout=timeout)

    def acknowledgeEventSummaries(self, userUuid, userName=None, event_filter=None, exclusionFilter=None, limit=None,
                                  timeout=None):
        update = from_dict(EventSummaryUpdate, dict(
            status = STATUS_ACKNOWLEDGED,
            current_user_uuid = userUuid,
            current_user_name = userName,
        ))
        return self.updateEventSummaries(update, event_filter=event_filter, exclusion_filter=exclusionFilter,
                                         limit=limit, timeout=timeout)

    def reopenEventSummaries(self, userUuid, userName=None, event_filter=None, exclusionFilter=None, limit=None,
                             timeout=None):
        update = from_dict(EventSummaryUpdate, dict(
            status = STATUS_NEW,
            current_user_uuid = userUuid,
            current_user_name = userName,
        ))
        return self.updateEventSummaries(update, event_filter=event_filter, exclusion_filter=exclusionFilter,
                                         limit=limit, timeout=timeout)

    def getEventTagSeverities(self, event_filter):
        return self.client.post('tag_severities', body=event_filter)

    def createSavedSearch(self, event_filter = None, exclusion_filter = None, sort = None, timeout = None,
                          archive = False):
        query = EventQuery()
        if event_filter:
            query.event_filter.MergeFrom(event_filter)
        if exclusion_filter:
            query.exclusion_filter.MergeFrom(exclusion_filter)
        if sort:
            for s in sort:
                query.sort.add().MergeFrom(s)
        if timeout:
            query.timeout = timeout
        url = 'search'
        if archive:
            url = 'archive/' + url
        status, response = self.client.post(url, query)
        if int(status['status']) != 201:
            raise ZepServiceException('Invalid response: %s (code: %s)' % (response, status['status']))
        location = status['location']
        uuid = location[location.rindex('/') + 1:]
        return uuid

    def deleteSavedSearch(self, search_uuid, archive = False):
        if not search_uuid:
            raise ValueError('Must specify UUID of search to delete')
        url = 'search/' + search_uuid
        if archive:
            url = 'archive/' + url
        return self.client.delete(url)

    def savedSearch(self, search_uuid, offset = 0, limit = None, archive = False):
        if not search_uuid:
            raise ValueError('Must specify UUID of search to perform')
        params = {}
        if offset:
            params['offset'] = offset
        if limit:
            params['limit'] = limit
        url = 'search/' + search_uuid
        if archive:
            url = 'archive/' + url
        return self.client.get(url, params)

    def updateDetails(self, uuid, eventDetailSet):
        return self.client.post(uuid + "/details", eventDetailSet)


class ZepConfigClient(object):

    _base_uri = '/zeneventserver/api/1.0/config/'

    def __init__(self, uri, queueSchema):
        self.client = ProtobufRestServiceClient(uri.rstrip('/') + self._base_uri, queueSchema,
                                                connection_error_class=ZepConnectionError)

    def defaultConfig(self, config):
        defaults = {}
        for field in ZepConfig.DESCRIPTOR.fields:
            defaults[field.name] = dict(defaultValue=field.default_value,
                                        value=getattr(config, field.name))
        return defaults

    def getConfig(self):
        response, config = self.client.get('')
        defaults = self.defaultConfig(config)
        return defaults

    def setConfigValues(self, config):
        """
        @param config: A ZepConfig protobuf object.
        """
        return self.client.put('', config)

    def setConfigValue(self, name, value):
        # zep expects every config item to be a string
        return self.client.put(name, value)

    def removeConfigValue(self, name):
        return self.client.delete(name)

    def getDetails(self):
        """
        Fetch the list of custom details that will be indexed. This will be a list of
        both Zenoss standard details as well as any details that have been added by zenpacks.
        """
        return self.client.get('index_details')

    def addIndexedDetails(self, detailItemSet):
        """
        @type detailItemSet: zenoss.protocols.protobufs.zep_pb2.EventDetailItemSet
        """
        return self.client.post('index_details', body=detailItemSet)

    def updateIndexedDetail(self, item):
        """
        @type item: zenoss.protocols.protobufs.zep_pb2.EventDetailItem
        """
        log.debug("Updating a detail item: '%s'" % item.key)
        return self.client.put('index_details/%s' % item.key, body=item)

    def removeIndexedDetail(self, key):
        """
        @type key: string
        """
        log.debug("Removing a detail item: '%s'" % key)
        return self.client.delete('index_details/%s' % key)


class ZepHeartbeatClient(object):

    _base_uri = '/zeneventserver/api/1.0/heartbeats/'

    def __init__(self, uri, queueSchema):
        self.client = ProtobufRestServiceClient(uri.rstrip('/') + self._base_uri, queueSchema,
                                                connection_error_class=ZepConnectionError)

    def getHeartbeats(self, monitor=None):
        uri = monitor if monitor else ''
        return self.client.get(uri)

    def deleteHeartbeats(self, monitor=None):
        uri = monitor if monitor else ''
        return self.client.delete(uri)

    def deleteHeartbeat(self, monitor, daemon):
        """
        Removes the heartbeat record for the specified monitor and daemon.

        @param monitor: The heartbeat monitor (i.e. 'localhost').
        @type monitor: basestring
        @param daemon: The heartbeat daemon (i.e. 'zenhub').
        @type daemon: basestring
        """
        uri = "/".join((monitor, daemon))
        return self.client.delete(uri)

class ZepEventTimeClient(object):

    _base_uri = '/zeneventserver/api/1.0/eventtime/'

    def __init__(self, uri, queueSchema):
        self.client = ProtobufRestServiceClient(uri.rstrip('/') + self._base_uri, queueSchema,
                                                connection_error_class=ZepConnectionError)

    def getEventTimesSince(self, timestamp=0, limit=1000):
        uri="since"
        params = dict(time=timestamp,
              limit=limit,)
        return self.client.get(uri, params)

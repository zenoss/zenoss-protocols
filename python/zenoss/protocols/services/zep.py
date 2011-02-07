import json
import time
import logging
from zenoss.protocols.services import ProtobufRestServiceClient, JsonRestServiceClient
from zenoss.protocols.jsonformat import to_dict, from_dict
from zenoss.protocols.protobufs.zep_pb2 import EventSummary, Event, EventNote, EventSummaryUpdate, EventSort, \
    EventSummaryUpdateRequest, EventSummaryRequest, EventQuery
from zenoss.protocols.protobufs.zep_pb2 import STATUS_NEW, STATUS_ACKNOWLEDGED, STATUS_CLOSED
from zenoss.protocols.protobufutil import ProtobufEnum, listify
from datetime import datetime, timedelta, tzinfo

log = logging.getLogger('zepclient')

EventSeverity = ProtobufEnum(Event, 'severity')
EventStatus = ProtobufEnum(EventSummary, 'status')
EventSortField = ProtobufEnum(EventSort, 'field')
EventSortDirection = ProtobufEnum(EventSort, 'direction')

ZERO = timedelta(0)
HOUR = timedelta(hours=1)

class ZepServiceException(Exception):
    pass

class ZepServiceClient(object):
    _base_uri = '/zenoss-zep/api/1.0/events/'
    _timeFormat = "%Y-%m-%dT%H:%M:%S.%fZ"

    def __init__(self, uri):
        self.client = ProtobufRestServiceClient(uri.rstrip('/') + self._base_uri)

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
        @param list keys List of keys to return
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

    def _updateEventSummaries(self, update, event_filter=None, exclusion_filter=None, limit=None):
        """
        @param update: EventSummaryUpdate protobuf
        @param event_filter: EventFilter protobuf
        @param exclusion_filter: EventFilter protobuf
        @param limit: integer
        """
        search_uuid = self.createSavedSearch(event_filter=event_filter, exclusion_filter=exclusion_filter)
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

    def closeEventSummaries(self, userUuid, userName=None, event_filter=None, exclusionFilter=None, limit=None):
        update = from_dict(EventSummaryUpdate, dict(
            status = STATUS_CLOSED,
        ))
        return self._updateEventSummaries(update, event_filter=event_filter, exclusion_filter=exclusionFilter,
                                          limit=limit)

    def acknowledgeEventSummaries(self, userUuid, userName=None, event_filter=None, exclusionFilter=None, limit=None):
        update = from_dict(EventSummaryUpdate, dict(
            status = STATUS_ACKNOWLEDGED,
            acknowledged_by_user_uuid = userUuid,
            acknowledged_by_user_name = userName,
        ))
        return self._updateEventSummaries(update, event_filter=event_filter, exclusion_filter=exclusionFilter,
                                          limit=limit)

    def reopenEventSummaries(self, userUuid, userName=None, event_filter=None, exclusionFilter=None, limit=None):
        update = from_dict(EventSummaryUpdate, dict(
            status = STATUS_NEW,
        ))
        return self._updateEventSummaries(update, event_filter=event_filter, exclusion_filter=exclusionFilter,
                                          limit=limit)

    def getEventSeverities(self, tagUuids):
        if not tagUuids:
            raise ValueError('At least one tag UUID must be provided.')

        return self.client.get('severities', params={ 'tag' : tagUuids })

    def getWorstSeverity(self, tagUuids):
        if not tagUuids:
            raise ValueError('At least one tag UUID must be provided.')

        return self.client.get('worst_severity', params={ 'tag' : tagUuids })

    def getDeviceIssues(self, filter):
        if filter:
            filterDict = to_dict(filter)
            if 'severity' in filterDict:
                filterDict['severity'] = [EventSeverity.getName(i) for i in filterDict['severity']]

            if 'status' in filterDict:
                filterDict['status'] = [EventStatus.getName(i) for i in filterDict['status']]

        return self.client.get('device_issues', params = filterDict)

    def getDetails(self):
        return self.client.get('details')

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

class ZepConfigClient(object):

    _base_uri = '/zenoss-zep/api/1.0/config/'

    def __init__(self, uri):
        self.client = JsonRestServiceClient(uri.rstrip('/') + self._base_uri)

    def defaultConfig(self, config):
        defaults = {
            'event_age_disable_severity': {
                'defaultValue': 'SEVERITY_ERROR',
                'value': config.get('event_age_disable_severity')
            },
            'event_age_interval_minutes': {
                'defaultValue': 4 * 60,
                'value': config.get('event_age_interval_minutes'),
            },
            'event_archive_purge_interval_days': {
                'defaultValue': 90,
                'value': config.get('event_archive_purge_interval_days')
            },
            'event_occurrence_purge_interval_days': {
                'defaultValue': 30,
                'value': config.get('event_occurrence_purge_interval_days')
            },
            'event_archive_interval_days': {
                'defaultValue': 3,
                'value': config.get('event_archive_interval_days')
            }
        }
        return defaults

    def getConfig(self):
        response, config = self.client.get('')
        defaults = self.defaultConfig(config)
        return defaults

    def setConfigValues(self, config):
        return self.client.post('', config)

    def setConfigValue(self, name, value):
        # zep expects every config item to be a string
        return self.client.post(name, value)

    def removeConfigValue(self, name):
        return self.client.delete(name)

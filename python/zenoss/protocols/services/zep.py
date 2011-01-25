import json
import time
import logging
from zenoss.protocols.services import ProtobufRestServiceClient, JsonRestServiceClient
from zenoss.protocols.jsonformat import to_dict, from_dict
from zenoss.protocols.protobufs.zep_pb2 import EventSummary, Event, EventNote, EventSummaryUpdate, EventSort, EventSummaryUpdateRequest, EventSummaryRequest
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

    def getEventSummary(self, uuid):
        """
        Get an event summary by event summary uuid.
        """
        return self.client.get('%s' % uuid)

    def updateEventSummaries(self, update, event_filter=None, exclusionFilter=None, updateTime=None, limit=None):
        """
        @param update: EventSummaryUpdate protobuf
        @param filter: EventFilter protobuf
        @param exclusionFilter: EventFilter protobuf
        @param updateTime: time
        @param limit: integer
        """
        updateRequestDict = dict(
            update_fields = to_dict(update),
        )

        if event_filter:
            updateRequestDict['event_filter'] = to_dict(event_filter)
            
            log.debug('Inside zep service, the event filter has become: %s', updateRequestDict['event_filter'])

        if exclusionFilter:
            log.debug('Found exclusion filter: ' + str(exclusionFilter))
            updateRequestDict['exclusion_filter'] = to_dict(exclusionFilter)
        if updateTime:
            updateRequestDict['update_time'] = updateTime
        else:
            # deliberately pass in None so that ZEP will use it's own updated
            # time.
            # updateRequestDict['update_time'] = None
            pass

        if limit is not None:
            updateRequestDict['limit'] = limit

        log.debug('issuing update request:' + str(updateRequestDict))

        updateRequest = from_dict(EventSummaryUpdateRequest, updateRequestDict)

        status, response = self.client.put('', body=updateRequest)
        return status, response

    def closeEventSummaries(self, userUuid, userName=None, event_filter=None, exclusionFilter=None, updateTime=None, limit=None):
        update = from_dict(EventSummaryUpdate, dict(
            status = STATUS_CLOSED,
            acknowledged_by_user_uuid = userUuid,
            acknowledged_by_user_name = userName,
        ))
        return self.updateEventSummaries(update, event_filter=event_filter,
            exclusionFilter=exclusionFilter, updateTime=updateTime, limit=limit)

    def acknowledgeEventSummaries(self, userUuid, userName=None, event_filter=None, exclusionFilter=None, updateTime=None, limit=None):
        update = from_dict(EventSummaryUpdate, dict(
            status = STATUS_ACKNOWLEDGED,
            acknowledged_by_user_uuid = userUuid,
            acknowledged_by_user_name = userName,
        ))
        return self.updateEventSummaries(update, event_filter=event_filter,
            exclusionFilter=exclusionFilter, updateTime=updateTime, limit=limit)

    def reopenEventSummaries(self, userUuid, userName=None, event_filter=None, exclusionFilter=None, updateTime=None, limit=None):
        update = from_dict(EventSummaryUpdate, dict(
            status = STATUS_NEW,
            acknowledged_by_user_uuid = userUuid,
            acknowledged_by_user_name = userName,
        ))
        return self.updateEventSummaries(update, event_filter=event_filter,
            exclusionFilter=exclusionFilter, updateTime=updateTime, limit=limit)

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

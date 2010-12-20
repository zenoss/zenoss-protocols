import json
from zenoss.protocols.services import ProtobufRestServiceClient, JsonRestServiceClient
from zenoss.protocols.jsonformat import to_dict, from_dict
from zenoss.protocols.protobufs.zep_pb2 import EventSummary, Event, NumberCondition, EventNote, EventSummaryUpdate, EventSort
from zenoss.protocols.protobufs.zep_pb2 import STATUS_NEW, STATUS_ACKNOWLEDGED, STATUS_CLOSED
from zenoss.protocols.protobufutil import ProtobufEnum
from datetime import datetime, timedelta, tzinfo

EventSeverity = ProtobufEnum(Event, 'severity')
EventStatus = ProtobufEnum(EventSummary, 'status')
EventSortField = ProtobufEnum(EventSort, 'field')
EventSortDirection = ProtobufEnum(EventSort, 'direction')

ZERO = timedelta(0)
HOUR = timedelta(hours=1)

# A UTC class.
class UTC(tzinfo):
    """UTC"""

    def utcoffset(self, dt):
        return ZERO

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return ZERO

utc = UTC()

class ZepServiceClient(object):
    _base_uri = '/zenoss-zep/api/1.0/events/'
    _timeFormat = "%Y-%m-%dT%H:%M:%S.%fZ"

    _opMap = {
        NumberCondition.LT : '<',
        NumberCondition.GT : '>',
        NumberCondition.GTEQ : '>=',
        NumberCondition.LTEQ : '<=',
        NumberCondition.EQ : '=',
        None : '=',
    }

    def __init__(self, uri):
        self.client = ProtobufRestServiceClient(uri.rstrip('/') + self._base_uri)

    def _timeRange(self, timeRange):
        values = []
        if 'start_time' in timeRange:
            date = datetime.utcfromtimestamp(timeRange['start_time'] / 1000).replace(tzinfo=utc)
            values.append(date.strftime(self._timeFormat))

        if 'end_time' in timeRange:
            date = datetime.utcfromtimestamp(timeRange['end_time'] / 1000).replace(tzinfo=utc)
            values.append(date.strftime(self._timeFormat))

        return '/'.join(values)

    def getEventSummariesFromArchive(self, offset=0, limit=100, keys=None, sort=None, filter=None):
        params = self._buildParams(offset, limit, keys, sort, filter)
        return self.client.get('archive', params=params)

    def getEventSummaries(self, offset=0, limit=100, keys=None, sort=None, filter=None):
        """
        Return a list of event summaries, optionally matching an EventFilter.

        @param filter EventSummaryFilter
        @param list keys List of keys to return
        """
        params = self._buildParams(offset, limit, keys, sort, filter)
        return self.client.get('', params=params)

    def _buildParams(self, offset=0, limit=100, keys=None, sort=None, filter=None):
        params = dict(
            offset = offset,
            limit = limit,
            keys = keys and ','.join(keys),
        )

        if filter:
            # Even though we require a filter protobuf we have to turn it back
            # into a dictionary
            filterDict = to_dict(filter)
            if 'severity' in filterDict:
                filterDict['severity'] = [EventSeverity.getName(i) for i in filterDict['severity']]

            if 'status' in filterDict:
                filterDict['status'] = [EventStatus.getName(i) for i in filterDict['status']]

            if 'count' in filterDict:
                filterDict['count'] = '%s%d' % (self._opMap[filterDict['count']['op']], filterDict['count']['value'])

            if 'first_seen' in filterDict:
                filterDict['first_seen'] = self._timeRange(filterDict['first_seen'])

            if 'last_seen' in filterDict:
                filterDict['last_seen'] = self._timeRange(filterDict['last_seen'])

            params.update(filterDict)

        if sort:
            if not isinstance(sort, list):
                sort = [sort]

            params['sort'] = []
            for eventSort in sort:
                params['sort'].append(EventSortField.getName(eventSort.field) + ',' + EventSortDirection.getName(eventSort.direction))

        return params

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

    def updateEventSummary(self, uuid, update):
        """
        Update an event summary by event summary uuid.
        """
        if not uuid:
            raise ValueError('UUID must be provided.')

        return self.client.put('%s' % uuid, body=update)

    def closeEventSummary(self, uuid):
        update = from_dict(EventSummaryUpdate, dict(
            status = STATUS_CLOSED
        ))

        return self.updateEventSummary(uuid, update)

    def acknowledgeEventSummary(self, uuid, userUuid):
        update = from_dict(EventSummaryUpdate, dict(
            status = STATUS_ACKNOWLEDGED,
            acknowledged_by_user_uuid = userUuid,
        ))

        return self.updateEventSummary(uuid, update)

    def reopenEventSummary(self, uuid):
        update = from_dict(EventSummaryUpdate, dict(
            status = STATUS_NEW
        ))

        return self.updateEventSummary(uuid, update)

    def getEventSeverities(self, tagUuids):
        if not tagUuids:
            raise ValueError('At least one tag UUID must be provided.')

        return self.client.get('severities', params={ 'tag' : tagUuids })


    def getWorstSeverity(self, tagUuids):
        if not tagUuids:
            raise ValueError('At least one tag UUID must be provided.')

        return self.client.get('worst_severity', params={ 'tag' : tagUuids })

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

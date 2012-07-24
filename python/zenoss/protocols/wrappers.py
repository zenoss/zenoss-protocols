##############################################################################
# 
# Copyright (C) Zenoss, Inc. 2011, all rights reserved.
# 
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
# 
##############################################################################


class EventSummaryAdapter(object):
    """A readonly view of event and eventSummary information, to be
       used in passing to various event evaluators and adapters that
       need access to current event information.
    """
    def __init__(self, evtsummary):
        self._es = evtsummary
        self._ev = evtsummary.occurrence[0]
        self._details = {}
        if self._ev.details:
            for det in self._ev.details:
                name = str(det.name)
                value = det.value
                if len(value) == 0:
                    value = ''
                elif len(value) == 1:
                    value = value[0]
                else:
                    value = list(value)
                self._details[name] = value

    @property
    def uuid(self):
        return self._es.uuid

    @property
    def occurrence(self):
        return self._es.occurrence

    @property
    def status(self):
        return self._es.status

    @property
    def first_seen_time(self):
        return self._es.first_seen_time

    @property
    def status_change_time(self):
        return self._es.status_change_time

    @property
    def last_seen_time(self):
        return self._es.last_seen_time

    @property
    def count(self):
        return self._es.count

    @property
    def current_user_uuid(self):
        return self._es.current_user_uuid

    @property
    def current_user_name(self):
        return self._es.current_user_name

    @property
    def cleared_by_event_uuid(self):
        return self._es.cleared_by_event_uuid

    @property
    def notes(self):
        return self._es.notes

    @property
    def audit_log(self):
        return self._es.audit_log

    @property
    def update_time(self):
        return self._es.update_time

    @property
    def created_time(self):
        return self._ev.created_time

    @property
    def fingerprint(self):
        return self._ev.fingerprint

    @property
    def event_class(self):
        return self._ev.event_class

    @property
    def event_class_key(self):
        return self._ev.event_class_key

    @property
    def event_class_mapping_uuid(self):
        return self._ev.event_class_mapping_uuid

    @property
    def actor(self):
        return self._ev.actor

    @property
    def summary(self):
        return self._ev.summary

    @property
    def message(self):
        return self._ev.message

    @property
    def severity(self):
        return self._ev.severity

    @property
    def event_key(self):
        return self._ev.event_key

    @property
    def event_group(self):
        return self._ev.event_group

    @property
    def agent(self):
        return self._ev.agent

    @property
    def syslog_priority(self):
        return self._ev.syslog_priority

    @property
    def syslog_facility(self):
        return self._ev.syslog_facility

    @property
    def nt_event_code(self):
        return self._ev.nt_event_code

    @property
    def monitor(self):
        return self._ev.monitor

    @property
    def tags(self):
        return self._ev.tags

    @property
    def details(self):
        return self._details

EventSummaryAdapter.FIELDS = [name for name in dir(EventSummaryAdapter)
                                if isinstance(getattr(EventSummaryAdapter, name), property)]

===========================
Zenoss Event Processing API
===========================

:Version: 1.0
:Base URL: /zeneventserver/api/$version/

------
Events
------

REST API
-------------

All responses from ZEP web services are returned as protobuf messages defined in zep.proto. The response will include a
X-Protobuf-FullName header containing the name of the protobuf message. Two content types are supported,
application/x-protobuf for binary encoded messages and application/json. The application/json encoding format is subject
to change - API clients should only use the binary encoded protobuf messages.

List Events
-----------

Get a list of events matching the given filters. Only the most recent occurrence of the event is returned.

Parameters
''''''''''
:sort: An `EventSort` to sort the results by. This is a comma-delimited value of "EventSort.Field,EventSort.Direction".
  The direction is optional (default is ASCENDING). (ex: "SEVERITY,DESCENDING", "SUMMARY").
:limit: The maximum number of events to retrieve (default: 100).
:offset: The result number to start at (for pagination).
    
Filter Options
''''''''''''''

See also `org.zenoss.protobufs.zep.EventSummaryRequest`
    
====================== ========= =======
Parameter              Multiples Values
====================== ========= =======
severity               Yes       CLEAR, DEBUG, INFO, WARNING, ERROR, CRITICAL
status                 Yes       NEW, ACKNOWLEDGED, SUPPRESSED, CLOSED, CLEARED, AGED
eventClass             Yes       Use `/Class/Name` for exact match or `/Class/Name/` to match that class and it's children
first_seen             No        ISO8601 [#iso8601]_ timestamp or timestamp range
last_seen              No        ISO8601 [#iso8601]_ timestamp or timestamp range
status_change          No        ISO8601 [#iso8601]_ timestamp or timestamp range
update_time            No        ISO8601 [#iso8601]_ timestamp or timestamp range
count                  No        Comparison condition (ex: "<4", ">3", ">=2", "3") or range (ex: "1:4", "0:100").
element_identifier     Yes       Element identifier. [#identifier]_
element_sub_identifier Yes       Sub element identifier. [#identifier]_
uuid                   Yes       An Event UUID
event_summary          Yes       Tokens to search within event summary (full text).
acknowledged_by_user   Yes       User names.
tag_uuids              Yes       UUIDs of tags stored in the event.
tag_uuids_op           No        AND, OR
====================== ========= =======

.. Note:: All of the above parameters can be prefixed with `ex_` to exclude events matching the condition. For example,
          specifying an `ex_severity` of `INFO` will exclude all events with severity INFO from the results.
    
.. Note:: Parameters that allow multiple values are OR-ed together (except for tag_uuids depending on the tag_uuids_op
          operator). Each parameter is AND-ed together with other parameters to build the query.
    
API
'''

Request::

    GET /zeneventserver/api/$version/events?$parameters
    Accept: application/x-protobuf
    
Response::

    Content-Type: application/x-protobuf
    X-Protobuf-FullName: org.zenoss.protobufs.zep.EventSummaryResult
    
Event Details
-------------

Get an event and all its details. Only the most recent occurrence of the event is returned.

API
'''

Request::

    GET /zeneventserver/api/$version/events/$eventUuid
    Accept: application/x-protobuf
    
Response::

    Content-Type: application/x-protobuf
    X-Protobuf-FullName: org.zenoss.protobufs.zep.EventSummary

Create Event
------------

To create an event, push the event into the raw events queue.

---------
Footnotes
---------
    
.. [#iso8601] `A standard date and time format <http://en.wikipedia.org/wiki/ISO_8601>`_,
              `Python implementation <http://code.google.com/p/pyiso8601/>`_. Example: "2007-06-20T12:34:40Z" or
              "2007-06-20T00:00:00Z/2007-07-20T00:00:00Z".
.. [#identifier] If the identifier query is enclosed in double quotes, a search is performed for the exact match. If
                 the identifier query is less than 3 characters in length or ends with an asterisk, a begins-with query
                 is performed. Otherwise, a substring query is performed.

===========================
Zenoss Event Processing API
===========================

:Version: 0.7
:Base URL: /zep/api/$version/

------
Events
------

List Events
-----------

Get a list of events matching the given filters. The event `details` will not be be in the response.

Parameters
''''''''''

:keys: A list of keys to populate in the request. All fields except `details` can be requested.
               `uuid` will always be provided.
:sort: An `EventSort` to sort the results by.
:limit: The maximum number of events to retrieve (default: 100).
:offset: The result number to start at (for pagination).
    
Filter Options
''''''''''''''

See also `org.zenoss.protobufs.zep.EventFilter`
    
=============== ========= =======
Property        Multiples Values
=============== ========= =======
severity        Yes       CLEAR, DEBUG, INFO, WARNING, ERROR, CRITICAL
status          Yes       NEW, ACKNOWLEDGED, SUPPRESSED
eventClass      Yes       Use `/Class/Name` for exact match or `/Class/Name/` to match that class and it's children
firstSeen       No        iso8601 [#iso8601]_ formatted timestamp or timestamp range (ex: "2007-06-20T12:34:40Z" or "2007-06-20T00:00:00Z/2007-07-20T00:00:00Z")
lastSeen        No        iso8601 [#iso8601]_ formatted timestamp or timestamp range (ex: "2007-06-20T12:34:40Z" or "2007-06-20T00:00:00Z/2007-07-20T00:00:00Z")
count           No        Comparison condition (ex: "<4", ">3", ">=2", "3")
device          Yes       A device GUID
component       Yes       A component GUID
service         Yes       A service GUID
uuid            Yes       An Event UUID
=============== ========= =======
    
.. Note:: Arguments that allow multiple values result in an `IN(sev1, sev2)` query.
    
API
'''

Request::

    GET /zep/api/$version/events?$parameters
    Accept: application/x-protobuf
    
Response::

    Content-Type: application/x-protobuf
    X-Protobuf-FullName: org.zenoss.protobufs.zep.EventSet
    
Event Details
-------------

Get an event and all its details.

API
'''

Request::

    GET /zep/api/$version/events/$eventUuid
    Accept: application/x-protobuf
    
Response::

    Content-Type: application/x-protobuf
    X-Protobuf-FullName: org.zenoss.protobufs.zep.Event

Create Event
------------

To create an event, push the event into the raw events queue.

-----
Model
-----

.. Note:: Not a whole lot of effort has been put into this section. It will need more thought and is not \
         needed yet.
         
Get a List of Elements
----------------------

Parameters
''''''''''

:keys: A list of keys to populate in the request.
:sort: 
:results: The number of results to get
:offset: The result number to start at (for pagination)
    
Filter Options
''''''''''''''
    
=============== ========= =======
Property        Multiples Values
=============== ========= =======
guid            Yes       An element GUID
=============== ========= =======
    
.. Note:: Arguments that allow multiple values result in an `IN(sev1, sev2)` query.

API
'''

Request::

    GET /zep/api/$version/model/$modelType?$parameters
    Accept: application/x-protobuf
    
Response::

    Content-Type: application/x-protobuf
    X-Protobuf-FullName: org.zenoss.protobufs.model.$ModelType
    
Get a Model Element
-------------------

Get a model element and the info that ZEP stores for it.

API
'''

Request::

    GET /zep/api/$version/model/$modelType/$guid
    Accept: application/x-protobuf
    
Response::

    Content-Type: application/x-protobuf
    X-Protobuf-FullName: org.zenoss.protobufs.model.$ModelType

Model Types
-----------

* Device
* Component
* Service [#esa]_

---------
Footnotes
---------
    
.. [#iso8601] `A standard date and time format <http://en.wikipedia.org/wiki/ISO_8601>`_, `Python implementation <http://code.google.com/p/pyiso8601/>`_
.. [#esa] Only available with ESA

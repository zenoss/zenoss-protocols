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

__import__('pkg_resources').declare_namespace(__name__)

from zenoss.protocols import queueschema
from zenoss.protocols.amqpconfig import setAMQPConfiguration


class InvalidQueueMessage(Exception):
    """
    Signals that the queue message received something other
    than a protobuf, which we currently do not support.
    """
    def __init__(self, value):
        self.reason = value
        super(InvalidQueueMessage, self).__init__(value)

def hydrateQueueMessage(message):
    """
    Process a queue message and return a fully hydrated protobuf class.
    @throws InvalidQueueMessage
    """
    contentType = message.properties.get('content_type', None)

    # check content type
    if not contentType or contentType != "application/x-protobuf":
        raise InvalidQueueMessage("%s is not a valid protobuf content type" % contentType)

    # make sure we have the full name
    if not message.application_headers.get('X-Protobuf-FullName'):
        raise InvalidQueueMessage("Message does not have a valid protobuf full name")

    fullName = message.application_headers['X-Protobuf-FullName']
    return queueschema.hydrateProtobuf(fullName, message.body)

# Deprecated alias
parse_protobuf = hydrateQueueMessage

def initializeAMQP(config):
    """
    Reads the queue config and then populates the queue config singleton
    """
    setAMQPConfiguration(config)


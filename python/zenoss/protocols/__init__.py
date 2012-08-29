##############################################################################
# 
# Copyright (C) Zenoss, Inc. 2010, all rights reserved.
# 
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
# 
##############################################################################


__import__('pkg_resources').declare_namespace(__name__)

from zenoss.protocols.adapters import registerAdapters
registerAdapters()

class InvalidQueueMessage(Exception):
    """
    Signals that the queue message received something other
    than a protobuf, which we currently do not support.
    """
    def __init__(self, value):
        self.reason = value
        super(InvalidQueueMessage, self).__init__(value)

def hydrateQueueMessage(message, queueSchema):
    """
    Process a queue message and return a fully hydrated protobuf class.
    @throws InvalidQueueMessage

    This method is designed to hydrate both messages from twisted and from eventlet.
    They come in slightly different formats.
    """
    if not hasattr(message, 'properties') and hasattr(message, 'content'):
        message = message.content

    properties = message.properties
    contentType = properties.get('content_type') or properties.get('content-type')

    # check content type
    if not contentType or contentType != "application/x-protobuf":
        raise InvalidQueueMessage("%s is not a valid protobuf content type" % contentType)

    fullName = None
    if hasattr(message, 'application_headers'):
        fullName = message.application_headers.get('X-Protobuf-FullName')
    else:
        fullName = properties.get('headers', {}).get('X-Protobuf-FullName')

    # make sure we have the full name
    if not fullName:
        raise InvalidQueueMessage("Message does not have a valid protobuf full name")

    return queueSchema.hydrateProtobuf(fullName, message.body)


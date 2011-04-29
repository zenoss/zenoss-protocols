###########################################################################
#
# This program is part of Zenoss Core, an open source monitoring platform.
# Copyright (C) 2010, Zenoss Inc.
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License version 2 or (at your
# option) any later version as published by the Free Software Foundation.
#
# For complete information please visit: http://www.zenoss.com/oss/
#
###########################################################################

from zenoss.protocols.services import ProtobufRestServiceClient
from zenoss.protocols.services.zep import ZepConnectionError
from zenoss.protocols.jsonformat import to_dict

class TriggerServiceClient(object):
    _base_uri = '/zeneventserver/api/1.0/triggers/'

    def __init__(self, uri):
        self.client = ProtobufRestServiceClient(uri.rstrip('/') + self._base_uri, connection_error_class=ZepConnectionError)
        
        
    def getTriggers(self):
        """
        Get all triggers.
        """
        return self.client.get('')
    
    def addTrigger(self, trigger):
        """
        @param trigger: The trigger to create.
        @type trigger: zenoss.protocols.protobufs.zep_pb2.EventTrigger
        """
        return self.client.put(trigger.uuid, trigger)
    
    def removeTrigger(self, uuid):
        """
        @param uuid: The uuid of the trigger to remove.
        @type uuid: string
        """
        return self.client.delete(uuid)
    
    def getTrigger(self, uuid):
        """
        @param uuid: The uuid of the trigger to retrieve.
        @type uuid: string
        """
        return self.client.get(uuid)
    
    def updateTrigger(self, trigger):
        """
        @param trigger: The trigger to update.
        @type trigger: zenoss.protocols.protobufs.zep_pb2.EventTrigger
        """
        return self.client.put(trigger.uuid, body=trigger)

    def updateSubscriptions(self, uuid, subscriptionSet):
        """
        @param uuid: the uuid of the subscriber
        @type uuid: string
        
        @param subscriptionSet: The set of subscriptions to update with.
        @type subscriptionSet: zenoss.protocols.protobufs.zep_pb2.EventTriggerSubscriptionSet
        """
        return self.client.put('subscriptions/%s' % uuid, body=subscriptionSet)

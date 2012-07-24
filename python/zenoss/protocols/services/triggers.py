##############################################################################
# 
# Copyright (C) Zenoss, Inc. 2010, all rights reserved.
# 
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
# 
##############################################################################


from zenoss.protocols.services import ProtobufRestServiceClient
from zenoss.protocols.services.zep import ZepConnectionError

class TriggerServiceClient(object):
    _base_uri = '/zeneventserver/api/1.0/triggers/'

    def __init__(self, uri, queueSchema):
        self.client = ProtobufRestServiceClient(uri.rstrip('/') + self._base_uri, queueSchema,
                                                connection_error_class=ZepConnectionError)
        
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

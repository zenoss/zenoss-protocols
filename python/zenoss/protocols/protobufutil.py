##############################################################################
# 
# Copyright (C) Zenoss, Inc. 2010, all rights reserved.
# 
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
# 
##############################################################################


class ProtobufEnum(object):
    """
    Helpful functions for protobuf enums.
    """
    def __init__(self, protobuf, field=None, enum=None):
        """

        """
        self._protobuf = protobuf

        if field:
            self._enum = self._protobuf.DESCRIPTOR.fields_by_name[field].enum_type
        elif enum:
            #protobuf should be the class that contains the named enum
            self._enum = self._protobuf.DESCRIPTOR.enum_types_by_name[enum]
        self._prefix = '_' in self._enum.values[0].name and self._enum.values[0].name.split('_', 1)[0] + '_' or None
        self._prettyNames = {}

        if self._prefix:
            getShortName = lambda val: val.name[len(self._prefix):] if val.name.startswith(self._prefix) else val.name
        else:
            getShortName = lambda val: val.name

        for val in self._enum.values:
            self._prettyNames[val.number] = getShortName(val).lower().replace('_', ' ').title()
            # Map the enum NAME to this class
            setattr(self, val.name, val.number)

    @property
    def numbers(self):
        """
        Return an ordered list of enum numbers
        """
        return list(sorted(v.number for v in self._enum.values))

    def getName(self, number):
        return self._enum.values_by_number[number].name

    def getNumber(self, name):
        return self._enum.values_by_name[name].number

    def getPrettyName(self, number):
        return self._prettyNames[number]

def listify(ob):
    """Helper function to make dealing with repeated fields slightly more tolerable.
    """
    if not isinstance(ob, (tuple, list, set)):
        return [ob]
    return ob

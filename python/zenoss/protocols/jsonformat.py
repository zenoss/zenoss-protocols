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

from json import dumps, loads
from google.protobuf.descriptor import FieldDescriptor

class ParseError(Exception):
  """Thrown in case of parsing error."""

_COMMON_FIELD_TYPE_MAP = {
    FieldDescriptor.TYPE_DOUBLE: float,
    FieldDescriptor.TYPE_FLOAT: float,
    FieldDescriptor.TYPE_INT64: long,
    FieldDescriptor.TYPE_UINT64: long,
    FieldDescriptor.TYPE_INT32: int,
    FieldDescriptor.TYPE_FIXED64: float,
    FieldDescriptor.TYPE_FIXED32: float,
    FieldDescriptor.TYPE_BOOL: bool,
    FieldDescriptor.TYPE_STRING: unicode,
    FieldDescriptor.TYPE_UINT32: int,
    FieldDescriptor.TYPE_ENUM: int,
    FieldDescriptor.TYPE_SFIXED32: float,
    FieldDescriptor.TYPE_SFIXED64: float,
    FieldDescriptor.TYPE_SINT32: int,
    FieldDescriptor.TYPE_SINT64: long
}

class Serializer(object):
    """
    Convert a protobuf instance into a json encoded protobuf.
    """
    _FIELD_TYPE_MAP = _COMMON_FIELD_TYPE_MAP
    _FIELD_TYPE_MAP[FieldDescriptor.TYPE_BYTES] = lambda x: x.encode('string_escape')

    def __init__(self):
        self._FIELD_TYPE_MAP[FieldDescriptor.TYPE_MESSAGE] = self

    def __call__(self, message):
        json = {}

        for field, value in message.ListFields():
            try:
                formatter = self._FIELD_TYPE_MAP[field.type]
            except KeyError:
                raise ParseError('Protobuf field "%s.%s" of type "%d" not supported.' % (message.__class__.__name__, field.name, field.type))

            if field.label == FieldDescriptor.LABEL_REPEATED:
                json[field.name] = [formatter(v) for v in value]
            else:
                json[field.name] = formatter(value)

        return json

to_dict = Serializer()

def to_json(message):
    return dumps(to_dict(message))

class Deserializer(object):
    """
    Convert a json encoded protobuf to a protobuf instance.
    """
    _FIELD_TYPE_MAP = _COMMON_FIELD_TYPE_MAP
    _FIELD_TYPE_MAP[FieldDescriptor.TYPE_BYTES] = lambda x: x.decode('string_escape')

    def _convert(self, protobuf, field, value):
        pb_value = getattr(protobuf, field.name, None)

        if field.type == FieldDescriptor.TYPE_MESSAGE:
            if field.label == FieldDescriptor.LABEL_REPEATED:
                for v in value:
                    self(pb_value.add(), v)
            else:
                self(pb_value, value)
        else:
            try:
                formatter = self._FIELD_TYPE_MAP[field.type]
            except KeyError:
                raise ParseError('Protobuf field "%s.%s" of type "%d" not supported.' % (message.__class__.__name__, field.name, field.type))

            if field.label == FieldDescriptor.LABEL_REPEATED:
                for v in value:
                    pb_value.append(formatter(v))
            else:
                setattr(protobuf, field.name, formatter(value))

    def __call__(self, protobuf, dict):
        if isinstance(protobuf, type):
            protobuf = protobuf()

        for field in protobuf.DESCRIPTOR.fields:
            if field.name in dict:
                try:
                    self._convert(protobuf, field, dict.get(field.name))
                except AttributeError:
                    raise AttributeError(
                            "Unable to get %s from %s %r" % (field.name,
                                                             type(dict), dict))
        return protobuf

from_dict = Deserializer()

def from_json(protobuf, message):
    return from_dict(protobuf, loads(message))


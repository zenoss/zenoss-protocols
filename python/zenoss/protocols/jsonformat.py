##############################################################################
# 
# Copyright (C) Zenoss, Inc. 2010, all rights reserved.
# 
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
# 
##############################################################################


from base64 import b64encode, b64decode
from json import dumps, loads
from google.protobuf.descriptor import FieldDescriptor
from .protobufutil import listify

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
    _FIELD_TYPE_MAP = dict(_COMMON_FIELD_TYPE_MAP)
    _FIELD_TYPE_MAP[FieldDescriptor.TYPE_BYTES] = b64encode

    def __init__(self):
        self._FIELD_TYPE_MAP[FieldDescriptor.TYPE_MESSAGE] = self

    def __call__(self, message):
        json = {}

        for field, value in message.ListFields():
            try:
                formatter = self._FIELD_TYPE_MAP[field.type]
            except KeyError:
                raise ParseError('Protobuf field "%s.%s" of type "%d" not supported.' % (message.__class__.__name__, field.name, field.type))

            field_name = field.full_name if field.is_extension else field.name
            if field.label == FieldDescriptor.LABEL_REPEATED:
                json[field_name] = [formatter(v) for v in value]
            else:
                json[field_name] = formatter(value)

        return json

to_dict = Serializer()

def to_json(message, indent=None):
    return dumps(to_dict(message), indent=indent)

class Deserializer(object):
    """
    Convert a json encoded protobuf to a protobuf instance.
    """
    _FIELD_TYPE_MAP = dict(_COMMON_FIELD_TYPE_MAP)
    _FIELD_TYPE_MAP[FieldDescriptor.TYPE_BYTES] = b64decode

    def _convert(self, protobuf, field, value):
        pb_value = getattr(protobuf, field.name, None)

        if field.type == FieldDescriptor.TYPE_MESSAGE:
            if field.label == FieldDescriptor.LABEL_REPEATED:
                for v in listify(value):
                    self(pb_value.add(), v)
            else:
                self(pb_value, value)
        else:
            try:
                formatter = self._FIELD_TYPE_MAP[field.type]
            except KeyError:
                raise ParseError('Protobuf field "%s.%s" of type "%d" not supported.' % (protobuf.__class__.__name__, field.name, field.type))

            if field.label == FieldDescriptor.LABEL_REPEATED:
                for v in listify(value):
                    pb_value.append(formatter(v))
            else:
                setattr(protobuf, field.name, formatter(value))

    def __call__(self, protobuf, dictvar):
        if isinstance(protobuf, type):
            protobuf = protobuf()

        for field in protobuf.DESCRIPTOR.fields:
            if field.name in dictvar:
                try:
                    self._convert(protobuf, field, dictvar.get(field.name))
                except AttributeError:
                    raise AttributeError(
                            "Unable to get %s from %s %r" % (field.name,
                                                             type(dictvar), dictvar))
        return protobuf

from_dict = Deserializer()

def from_json(protobuf, message):
    return from_dict(protobuf, loads(message))

from typing import ClassVar as _ClassVar
from typing import Optional as _Optional

from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message

DESCRIPTOR: _descriptor.FileDescriptor

class DecimalValue(_message.Message):
    __slots__ = ("units", "nanos")
    UNITS_FIELD_NUMBER: _ClassVar[int]
    NANOS_FIELD_NUMBER: _ClassVar[int]
    units: int
    nanos: int
    def __init__(self, units: _Optional[int] = ..., nanos: _Optional[int] = ...) -> None: ...

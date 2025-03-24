from typing import ClassVar as _ClassVar
from typing import Optional as _Optional

from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message

DESCRIPTOR: _descriptor.FileDescriptor

class Brs021ForwardMeteredDataNotifyV1(_message.Message):
    __slots__ = ("version", "orchestration_instance_id")
    VERSION_FIELD_NUMBER: _ClassVar[int]
    ORCHESTRATION_INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    version: str
    orchestration_instance_id: str
    def __init__(self, version: _Optional[str] = ..., orchestration_instance_id: _Optional[str] = ...) -> None: ...

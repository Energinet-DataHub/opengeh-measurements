from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class VersionMessage(_message.Message):
    __slots__ = ("Version",)
    VERSION_FIELD_NUMBER: _ClassVar[int]
    Version: str
    def __init__(self, Version: _Optional[str] = ...) -> None: ...

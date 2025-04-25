from typing import ClassVar as _ClassVar
from typing import Optional as _Optional

from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message

DESCRIPTOR: _descriptor.FileDescriptor

class VersionMessage(_message.Message):
    __slots__ = ("Version",)
    VERSION_FIELD_NUMBER: _ClassVar[int]
    Version: str
    def __init__(self, Version: _Optional[str] = ...) -> None: ...

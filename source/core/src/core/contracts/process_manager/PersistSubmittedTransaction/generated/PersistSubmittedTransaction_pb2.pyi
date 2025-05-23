from collections.abc import Iterable as _Iterable
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar
from typing import Optional as _Optional
from typing import Union as _Union

from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper

import core.contracts.process_manager.PersistSubmittedTransaction.generated.DecimalValue_pb2 as _DecimalValue_pb2

DESCRIPTOR: _descriptor.FileDescriptor

class OrchestrationType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    OT_UNSPECIFIED: _ClassVar[OrchestrationType]
    OT_SUBMITTED_MEASURE_DATA: _ClassVar[OrchestrationType]

class Quality(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    Q_UNSPECIFIED: _ClassVar[Quality]
    Q_MISSING: _ClassVar[Quality]
    Q_ESTIMATED: _ClassVar[Quality]
    Q_MEASURED: _ClassVar[Quality]
    Q_CALCULATED: _ClassVar[Quality]

class MeteringPointType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    MPT_UNSPECIFIED: _ClassVar[MeteringPointType]
    MPT_CONSUMPTION: _ClassVar[MeteringPointType]
    MPT_PRODUCTION: _ClassVar[MeteringPointType]
    MPT_EXCHANGE: _ClassVar[MeteringPointType]
    MPT_VE_PRODUCTION: _ClassVar[MeteringPointType]
    MPT_ANALYSIS: _ClassVar[MeteringPointType]
    MPT_NOT_USED: _ClassVar[MeteringPointType]
    MPT_SURPLUS_PRODUCTION_GROUP_6: _ClassVar[MeteringPointType]
    MPT_NET_PRODUCTION: _ClassVar[MeteringPointType]
    MPT_SUPPLY_TO_GRID: _ClassVar[MeteringPointType]
    MPT_CONSUMPTION_FROM_GRID: _ClassVar[MeteringPointType]
    MPT_WHOLESALE_SERVICES_INFORMATION: _ClassVar[MeteringPointType]
    MPT_OWN_PRODUCTION: _ClassVar[MeteringPointType]
    MPT_NET_FROM_GRID: _ClassVar[MeteringPointType]
    MPT_NET_TO_GRID: _ClassVar[MeteringPointType]
    MPT_TOTAL_CONSUMPTION: _ClassVar[MeteringPointType]
    MPT_NET_LOSS_CORRECTION: _ClassVar[MeteringPointType]
    MPT_ELECTRICAL_HEATING: _ClassVar[MeteringPointType]
    MPT_NET_CONSUMPTION: _ClassVar[MeteringPointType]
    MPT_OTHER_CONSUMPTION: _ClassVar[MeteringPointType]
    MPT_OTHER_PRODUCTION: _ClassVar[MeteringPointType]
    MPT_CAPACITY_SETTLEMENT: _ClassVar[MeteringPointType]
    MPT_EXCHANGE_REACTIVE_ENERGY: _ClassVar[MeteringPointType]
    MPT_COLLECTIVE_NET_PRODUCTION: _ClassVar[MeteringPointType]
    MPT_COLLECTIVE_NET_CONSUMPTION: _ClassVar[MeteringPointType]
    MPT_INTERNAL_USE: _ClassVar[MeteringPointType]

class Unit(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    U_UNSPECIFIED: _ClassVar[Unit]
    U_KWH: _ClassVar[Unit]
    U_KW: _ClassVar[Unit]
    U_MW: _ClassVar[Unit]
    U_MWH: _ClassVar[Unit]
    U_TONNE: _ClassVar[Unit]
    U_KVARH: _ClassVar[Unit]
    U_MVAR: _ClassVar[Unit]

class Resolution(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    R_UNSPECIFIED: _ClassVar[Resolution]
    R_PT15M: _ClassVar[Resolution]
    R_PT1H: _ClassVar[Resolution]
    R_P1M: _ClassVar[Resolution]

OT_UNSPECIFIED: OrchestrationType
OT_SUBMITTED_MEASURE_DATA: OrchestrationType
Q_UNSPECIFIED: Quality
Q_MISSING: Quality
Q_ESTIMATED: Quality
Q_MEASURED: Quality
Q_CALCULATED: Quality
MPT_UNSPECIFIED: MeteringPointType
MPT_CONSUMPTION: MeteringPointType
MPT_PRODUCTION: MeteringPointType
MPT_EXCHANGE: MeteringPointType
MPT_VE_PRODUCTION: MeteringPointType
MPT_ANALYSIS: MeteringPointType
MPT_NOT_USED: MeteringPointType
MPT_SURPLUS_PRODUCTION_GROUP_6: MeteringPointType
MPT_NET_PRODUCTION: MeteringPointType
MPT_SUPPLY_TO_GRID: MeteringPointType
MPT_CONSUMPTION_FROM_GRID: MeteringPointType
MPT_WHOLESALE_SERVICES_INFORMATION: MeteringPointType
MPT_OWN_PRODUCTION: MeteringPointType
MPT_NET_FROM_GRID: MeteringPointType
MPT_NET_TO_GRID: MeteringPointType
MPT_TOTAL_CONSUMPTION: MeteringPointType
MPT_NET_LOSS_CORRECTION: MeteringPointType
MPT_ELECTRICAL_HEATING: MeteringPointType
MPT_NET_CONSUMPTION: MeteringPointType
MPT_OTHER_CONSUMPTION: MeteringPointType
MPT_OTHER_PRODUCTION: MeteringPointType
MPT_CAPACITY_SETTLEMENT: MeteringPointType
MPT_EXCHANGE_REACTIVE_ENERGY: MeteringPointType
MPT_COLLECTIVE_NET_PRODUCTION: MeteringPointType
MPT_COLLECTIVE_NET_CONSUMPTION: MeteringPointType
MPT_INTERNAL_USE: MeteringPointType
U_UNSPECIFIED: Unit
U_KWH: Unit
U_KW: Unit
U_MW: Unit
U_MWH: Unit
U_TONNE: Unit
U_KVARH: Unit
U_MVAR: Unit
R_UNSPECIFIED: Resolution
R_PT15M: Resolution
R_PT1H: Resolution
R_P1M: Resolution

class PersistSubmittedTransaction(_message.Message):
    __slots__ = (
        "version",
        "orchestration_instance_id",
        "orchestration_type",
        "metering_point_id",
        "transaction_id",
        "transaction_creation_datetime",
        "start_datetime",
        "end_datetime",
        "metering_point_type",
        "unit",
        "resolution",
        "points",
    )
    VERSION_FIELD_NUMBER: _ClassVar[int]
    ORCHESTRATION_INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    ORCHESTRATION_TYPE_FIELD_NUMBER: _ClassVar[int]
    METERING_POINT_ID_FIELD_NUMBER: _ClassVar[int]
    TRANSACTION_ID_FIELD_NUMBER: _ClassVar[int]
    TRANSACTION_CREATION_DATETIME_FIELD_NUMBER: _ClassVar[int]
    START_DATETIME_FIELD_NUMBER: _ClassVar[int]
    END_DATETIME_FIELD_NUMBER: _ClassVar[int]
    METERING_POINT_TYPE_FIELD_NUMBER: _ClassVar[int]
    UNIT_FIELD_NUMBER: _ClassVar[int]
    RESOLUTION_FIELD_NUMBER: _ClassVar[int]
    POINTS_FIELD_NUMBER: _ClassVar[int]
    version: str
    orchestration_instance_id: str
    orchestration_type: OrchestrationType
    metering_point_id: str
    transaction_id: str
    transaction_creation_datetime: _timestamp_pb2.Timestamp
    start_datetime: _timestamp_pb2.Timestamp
    end_datetime: _timestamp_pb2.Timestamp
    metering_point_type: MeteringPointType
    unit: Unit
    resolution: Resolution
    points: _containers.RepeatedCompositeFieldContainer[Point]
    def __init__(
        self,
        version: _Optional[str] = ...,
        orchestration_instance_id: _Optional[str] = ...,
        orchestration_type: _Optional[_Union[OrchestrationType, str]] = ...,
        metering_point_id: _Optional[str] = ...,
        transaction_id: _Optional[str] = ...,
        transaction_creation_datetime: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...,
        start_datetime: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...,
        end_datetime: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...,
        metering_point_type: _Optional[_Union[MeteringPointType, str]] = ...,
        unit: _Optional[_Union[Unit, str]] = ...,
        resolution: _Optional[_Union[Resolution, str]] = ...,
        points: _Optional[_Iterable[_Union[Point, _Mapping]]] = ...,
    ) -> None: ...

class Point(_message.Message):
    __slots__ = ("position", "quantity", "quality")
    POSITION_FIELD_NUMBER: _ClassVar[int]
    QUANTITY_FIELD_NUMBER: _ClassVar[int]
    QUALITY_FIELD_NUMBER: _ClassVar[int]
    position: int
    quantity: _DecimalValue_pb2.DecimalValue
    quality: Quality
    def __init__(
        self,
        position: _Optional[int] = ...,
        quantity: _Optional[_Union[_DecimalValue_pb2.DecimalValue, _Mapping]] = ...,
        quality: _Optional[_Union[Quality, str]] = ...,
    ) -> None: ...

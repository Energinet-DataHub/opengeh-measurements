# ProtoBuf doesn't support decimal.
# This implementation is inspired by https://docs.microsoft.com/en-us/aspnet/core/grpc/protobuf?view=aspnetcore-5.0#decimals.
#
# units: Whole units part of the amount
# nanos: Nano units of the amount (10^-9). Must be same sign as units
#
# Example: 12345.6789 -> { units = 12345, nanos = 678900000 }


class DecimalValue:
    NanoFactor = 1_000_000_000

    def __init__(self, units, nanos):
        self.units = units
        self.nanos = nanos

    @staticmethod
    def from_decimal(value):
        units = int(value)
        nanos = int((value - units) * DecimalValue.NanoFactor)
        return DecimalValue(units, nanos)

    def to_decimal(self):
        return self.units + self.nanos / DecimalValue.NanoFactor

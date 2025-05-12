using NodaTime;

namespace Energinet.DataHub.Measurements.Domain;

public record MeasurementAggregationByPeriod(
    YearMonth YearMonth,
    decimal Quantity,
    Quality Quality,
    Unit Unit);

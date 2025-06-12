using NodaTime;

namespace Energinet.DataHub.Measurements.Domain;

[Obsolete("Use MeasurementAggregationByMonth instead.")]
public record MeasurementAggregationByMonthV4(
    YearMonth YearMonth,
    decimal? Quantity,
    Quality Quality,
    Unit Unit);

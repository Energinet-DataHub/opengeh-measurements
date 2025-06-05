using NodaTime;

namespace Energinet.DataHub.Measurements.Domain;

public record MeasurementAggregationByMonth(
    YearMonth YearMonth,
    decimal? Quantity,
    Unit Unit);

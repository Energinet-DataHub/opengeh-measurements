using NodaTime;

namespace Energinet.DataHub.Measurements.Domain;

public record MeasurementAggregationByMonth(
    YearMonth YearMonth,
    decimal Quantity,
    Quality Quality,
    Unit Unit,
    bool MissingValues,
    bool ContainsUpdatedValues);

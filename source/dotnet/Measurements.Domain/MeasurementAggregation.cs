using NodaTime;

namespace Energinet.DataHub.Measurements.Domain;

public record MeasurementAggregation(DateOnly Date, decimal Quantity, Quality Quality, bool MissingValues);

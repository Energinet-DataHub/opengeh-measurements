using NodaTime;

namespace Energinet.DataHub.Measurements.Domain;

public record MeasurementAggregation(LocalDate Date, decimal Quantity, Quality Quality, bool MissingValues);

using NodaTime;

namespace Energinet.DataHub.Measurements.Domain;

public record MeasurementAggregation(Instant MinObservationTime, Instant MaxObservationTime, decimal Quantity, Quality Quality, Unit Unit);

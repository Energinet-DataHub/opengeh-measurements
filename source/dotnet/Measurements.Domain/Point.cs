using NodaTime;

namespace Energinet.DataHub.Measurements.Domain;

public record Point(Instant ObservationTime, decimal Quantity, Quality Quality, Unit Unit);

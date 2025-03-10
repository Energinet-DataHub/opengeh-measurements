namespace Energinet.DataHub.Measurements.Domain;

public record Point(DateTimeOffset ObservationTime, decimal Quantity, Quality Quality);

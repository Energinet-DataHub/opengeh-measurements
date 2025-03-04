namespace Energinet.DataHub.Measurements.Domain;

public record Transaction(
    DateTimeOffset StartTimestamp,
    DateTimeOffset EndTimestamp,
    decimal Quantity,
    string Unit,
    string Quality);

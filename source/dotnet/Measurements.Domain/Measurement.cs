namespace Energinet.DataHub.Measurements.Domain;

public record Measurement(IEnumerable<Transaction> Transactions);

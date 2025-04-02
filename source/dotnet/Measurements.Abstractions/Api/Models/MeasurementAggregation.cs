namespace Energinet.DataHub.Measurements.Abstractions.Api.Models;

public record MeasurementAggregation(DateTimeOffset MinObservationTime, DateTimeOffset MaxObservationTime, decimal Quantity, IEnumerable<Quality> Qualities, int PointCount);

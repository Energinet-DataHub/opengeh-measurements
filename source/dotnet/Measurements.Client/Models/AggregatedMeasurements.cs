using Energinet.DataHub.Measurements.Abstractions.Api.Models;

namespace Energinet.DataHub.Measurements.Client.Models;

public record AggregatedMeasurements(DateTimeOffset MinObservationTime, DateTimeOffset MaxObservationTime, decimal Quantity, IEnumerable<Quality> Qualities, int PointCount);

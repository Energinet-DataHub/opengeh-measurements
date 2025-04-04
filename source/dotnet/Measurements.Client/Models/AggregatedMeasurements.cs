using Energinet.DataHub.Measurements.Abstractions.Api.Models;

namespace Energinet.DataHub.Measurements.Client.Models;

public sealed record AggregatedMeasurements(
    DateTimeOffset MinObservationTime,
    DateTimeOffset MaxObservationTime,
    decimal Quantity,
    IEnumerable<Quality> Qualities,
    IEnumerable<Resolution> Resolutions,
    int PointCount);

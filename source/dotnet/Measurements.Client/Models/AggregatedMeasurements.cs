namespace Energinet.DataHub.Measurements.Client.Models;

internal sealed record AggregatedMeasurements(
    DateTimeOffset MinObservationTime,
    DateTimeOffset MaxObservationTime,
    decimal Quantity,
    IEnumerable<Resolution> Resolutions,
    int PointCount);

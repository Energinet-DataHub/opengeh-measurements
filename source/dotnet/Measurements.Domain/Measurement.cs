namespace Energinet.DataHub.Measurements.Domain;

public record Measurement(
    string MeteringPointId,
    Unit Unit,
    IEnumerable<Point> Points);

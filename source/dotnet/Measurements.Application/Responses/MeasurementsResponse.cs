using System.ComponentModel;
using System.Text.Json.Serialization;
using Energinet.DataHub.Measurements.Application.Persistence;
using Energinet.DataHub.Measurements.Application.Responses.EnumParsers;
using Energinet.DataHub.Measurements.Domain;

namespace Energinet.DataHub.Measurements.Application.Responses;

public class MeasurementsResponse
{
    // ReSharper disable once AutoPropertyCanBeMadeGetOnly.Global - used by System.Text.Json
    public IReadOnlyCollection<Point> Points { get; init; } = [];

    [JsonConstructor]
    [Browsable(false)]
    private MeasurementsResponse() { } // Needed by System.Text.Json to deserialize

    private MeasurementsResponse(IReadOnlyCollection<Point> points)
    {
        Points = points;
    }

    public static MeasurementsResponse? Create(IEnumerable<MeasurementResult> measurements)
    {
        var points = measurements
            .Select(measurement =>
                new Point(
                    measurement.ObservationTime,
                    measurement.Quantity,
                    QualityParser.ParseQuality(measurement.Quality),
                    UnitParser.ParseUnit(measurement.Unit),
                    ResolutionParser.ParseResolution(measurement.Resolution),
                    measurement.Created,
                    measurement.TransactionCreated))
            .ToList();

        return points.Count <= 0
            ? null : new MeasurementsResponse(points);
    }
}

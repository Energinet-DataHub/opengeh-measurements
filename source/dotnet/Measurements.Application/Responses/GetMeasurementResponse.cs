using System.ComponentModel;
using System.Text.Json.Serialization;
using Energinet.DataHub.Measurements.Application.Exceptions;
using Energinet.DataHub.Measurements.Application.Persistence;
using Energinet.DataHub.Measurements.Domain;

namespace Energinet.DataHub.Measurements.Application.Responses;

public class GetMeasurementResponse
{
    // ReSharper disable once AutoPropertyCanBeMadeGetOnly.Global - used by System.Text.Json
    public IReadOnlyCollection<Point> Points { get; init; } = [];

    [JsonConstructor]
    [Browsable(false)]
    private GetMeasurementResponse() { } // Needed by System.Text.Json to deserialize

    private GetMeasurementResponse(IReadOnlyCollection<Point> points)
    {
        Points = points;
    }

    public static GetMeasurementResponse Create(IEnumerable<MeasurementResult> measurements)
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
            ? throw new MeasurementsNotFoundException()
            : new GetMeasurementResponse(points);
    }
}

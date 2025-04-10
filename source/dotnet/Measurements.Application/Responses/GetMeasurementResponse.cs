using System.ComponentModel;
using System.Text.Json.Serialization;
using Energinet.DataHub.Measurements.Application.Exceptions;
using Energinet.DataHub.Measurements.Application.Persistence;
using Energinet.DataHub.Measurements.Domain;
using NodaTime;

namespace Energinet.DataHub.Measurements.Application.Responses;

public class GetMeasurementResponse
{
    public IReadOnlyCollection<Point> Points { get; init; } = [];

    public SortedList<Instant, Point> Points2 { get; init; } = [];

    [JsonConstructor]
    [Browsable(false)]
    private GetMeasurementResponse() { } // Needed by System.Text.Json to deserialize

    private GetMeasurementResponse(List<Point> points)
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
                    measurement.Created))
            .ToList();

        foreach (var measurement in measurements)
        {

        }

        return points.Count <= 0
            ? throw new MeasurementsNotFoundDuringPeriodException()
            : new GetMeasurementResponse(points);
    }
}

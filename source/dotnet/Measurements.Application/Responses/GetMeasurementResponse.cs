using System.ComponentModel;
using System.Text.Json.Serialization;
using Energinet.DataHub.Measurements.Application.Persistence;
using Energinet.DataHub.Measurements.Domain;

namespace Energinet.DataHub.Measurements.Application.Responses;

public class GetMeasurementResponse
{
    public string MeteringPointId { get; init; }

    public Unit Unit { get; init; }

    public IReadOnlyCollection<Point> Points { get; init; }

    [JsonConstructor]
    [Browsable(false)]
    public GetMeasurementResponse(string meteringPointId, Unit unit, List<Point> points)
    {
        MeteringPointId = meteringPointId;
        Unit = unit;
        Points = points;
    }

    public static GetMeasurementResponse Create(IEnumerable<MeasurementResult> measurements)
    {
        var meteringPointId = string.Empty;
        var unit = Unit.Unknown;
        var points = new List<Point>();

        foreach (var measurement in measurements)
        {
            meteringPointId = measurement.MeteringPointId;
            unit = ParseUnit(measurement.Unit);
            points.Add(new Point(
                measurement.ObservationTime,
                measurement.Quantity,
                ParseQuality(measurement.Quality)));
        }

        return meteringPointId == string.Empty || unit == Unit.Unknown || points.Count <= 0
            ? throw new Exception("Measurement could not be created from result.")
            : new GetMeasurementResponse(meteringPointId, unit, points);
    }

    private static Quality ParseQuality(string quality)
    {
        return quality switch
        {
            "measured" => Quality.Measured,
            "estimated" => Quality.Estimated,
            "calculated" => Quality.Calculated,
            _ => throw new Exception("Unknown quality"),
        };
    }

    private static Unit ParseUnit(string unit)
    {
        return unit switch
        {
            "KWH" => Unit.kWh,
            "MWH" => Unit.MWh,
            _ => throw new Exception("Unknown unit"),
        };
    }
}

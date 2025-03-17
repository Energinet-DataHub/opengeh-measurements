using System.ComponentModel;
using System.Text.Json.Serialization;
using Energinet.DataHub.Measurements.Application.Exceptions;
using Energinet.DataHub.Measurements.Application.Persistence;
using Energinet.DataHub.Measurements.Domain;

namespace Energinet.DataHub.Measurements.Application.Responses;

public class GetMeasurementResponse
{
    public string MeteringPointId { get; init; } = string.Empty;

    public IReadOnlyCollection<Point> Points { get; init; } = [];

    [JsonConstructor]
    [Browsable(false)]
    private GetMeasurementResponse() { } // Needed by System.Text.Json to deserialize

    private GetMeasurementResponse(string meteringPointId, List<Point> points)
    {
        MeteringPointId = meteringPointId;
        Points = points;
    }

    public static GetMeasurementResponse Create(IEnumerable<MeasurementsResult> measurements)
    {
        var meteringPointId = string.Empty;
        var points = new List<Point>();

        foreach (var measurement in measurements)
        {
            meteringPointId = measurement.MeteringPointId;
            points.Add(new Point(
                measurement.ObservationTime,
                measurement.Quantity,
                ParseQuality(measurement.Quality),
                ParseUnit(measurement.Unit)));
        }

        return meteringPointId == string.Empty || points.Count <= 0
            ? throw new MeasurementsNotFoundException()
            : new GetMeasurementResponse(meteringPointId, points);
    }

    private static Quality ParseQuality(string quality)
    {
        return quality.ToLower() switch
        {
            "missing" => Quality.Missing,
            "estimated" => Quality.Estimated,
            "measured" => Quality.Measured,
            "calculated" => Quality.Calculated,
            _ => throw new ArgumentOutOfRangeException(nameof(quality)),
        };
    }

    private static Unit ParseUnit(string unit)
    {
        return unit.ToLower() switch
        {
            "kwh" => Unit.kWh,
            "kw" => Unit.kW,
            "mw" => Unit.MW,
            "mwh" => Unit.MWh,
            "tonne" => Unit.Tonne,
            "kvarh" => Unit.kVArh,
            "mvar" => Unit.MVAr,
            _ => throw new ArgumentOutOfRangeException(nameof(unit)),
        };
    }
}

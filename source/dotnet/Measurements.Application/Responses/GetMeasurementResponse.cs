using System.ComponentModel;
using System.Text.Json.Serialization;
using Energinet.DataHub.Measurements.Application.Exceptions;
using Energinet.DataHub.Measurements.Application.Persistence;
using Energinet.DataHub.Measurements.Domain;

namespace Energinet.DataHub.Measurements.Application.Responses;

public class GetMeasurementResponse
{
    public string MeteringPointId { get; init; } = string.Empty;

    public Unit Unit { get; init; }

    public IReadOnlyCollection<Point> Points { get; init; } = [];

    [JsonConstructor]
    [Browsable(false)]
    private GetMeasurementResponse() { } // Needed by System.Text.Json to deserialize

    private GetMeasurementResponse(string meteringPointId, Unit unit, List<Point> points)
    {
        MeteringPointId = meteringPointId;
        Unit = unit;
        Points = points;
    }

    public static GetMeasurementResponse Create(IEnumerable<MeasurementsResult> measurements)
    {
        var meteringPointId = string.Empty;
        var unitRaw = string.Empty;
        var points = new List<Point>();

        foreach (var measurement in measurements)
        {
            meteringPointId = measurement.MeteringPointId;
            unitRaw = measurement.Unit;
            points.Add(new Point(
                measurement.ObservationTime,
                measurement.Quantity,
                ParseQuality(measurement.Quality)));
        }

        if (meteringPointId == string.Empty || points.Count <= 0)
        {
            throw new MeasurementsNotFoundException();
        }

        var unit = ParseUnit(unitRaw);

        return new GetMeasurementResponse(meteringPointId, unit, points);
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

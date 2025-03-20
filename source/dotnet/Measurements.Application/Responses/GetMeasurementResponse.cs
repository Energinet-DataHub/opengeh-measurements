using System.ComponentModel;
using System.Text.Json.Serialization;
using Energinet.DataHub.Measurements.Application.Exceptions;
using Energinet.DataHub.Measurements.Application.Persistence;
using Energinet.DataHub.Measurements.Domain;

namespace Energinet.DataHub.Measurements.Application.Responses;

public class GetMeasurementResponse
{
    public IReadOnlyCollection<Point> Points { get; init; } = [];

    [JsonConstructor]
    [Browsable(false)]
    private GetMeasurementResponse() { } // Needed by System.Text.Json to deserialize

    private GetMeasurementResponse(List<Point> points)
    {
        Points = points;
    }

    public static GetMeasurementResponse Create(IEnumerable<MeasurementsResult> measurements)
    {
        var points = measurements
            .Select(measurement =>
                new Point(
                    measurement.ObservationTime,
                    measurement.Quantity,
                    ParseQuality(measurement.Quality),
                    ParseUnit(measurement.Unit)))
            .ToList();

        return points.Count <= 0
            ? throw new MeasurementsNotFoundDuringPeriodException()
            : new GetMeasurementResponse(points);
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

using Energinet.DataHub.Measurements.Application.Persistence;
using Energinet.DataHub.Measurements.Domain;

namespace Energinet.DataHub.Measurements.Application.Responses;

public class GetMeasurementResponse
{
    public string MeteringPointId { get; private set; }

    public Unit Unit { get; private set; }

    public IReadOnlyCollection<Point> Points { get; private set; }

    private GetMeasurementResponse(string meteringPointId, Unit unit, List<Point> points)
    {
        MeteringPointId = meteringPointId;
        Unit = unit;
        Points = points;
    }

    public static async Task<GetMeasurementResponse> CreateAsync(IAsyncEnumerable<MeasurementResult> measurements)
    {
        var meteringPointId = string.Empty;
        var unit = Unit.Unknown;
        var points = new List<Point>();

        await foreach (var measurement in measurements)
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
            "Measured" => Quality.Measured,
            "Estimated" => Quality.Estimated,
            _ => throw new Exception("Unknown quality"),
        };
    }

    private static Unit ParseUnit(string unit)
    {
        return unit switch
        {
            "KWh" => Unit.KWh,
            "MWh" => Unit.MWh,
            _ => throw new Exception("Unknown unit"),
        };
    }
}

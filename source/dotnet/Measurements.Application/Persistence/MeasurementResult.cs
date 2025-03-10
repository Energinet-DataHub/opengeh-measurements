using System.Dynamic;

namespace Energinet.DataHub.Measurements.Application.Persistence;

public class MeasurementResult(ExpandoObject raw)
{
    private readonly dynamic _raw = raw;

    public string MeteringPointId => _raw.metering_point_id;

    public string Unit => "KWH"; // _raw.unit;

    public DateTimeOffset ObservationTime => _raw.observation_time;

    public decimal Quantity => _raw.quantity;

    public string Quality => _raw.quality;
}

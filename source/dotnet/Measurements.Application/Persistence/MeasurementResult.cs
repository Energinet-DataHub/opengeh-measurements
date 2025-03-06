using System.Dynamic;
using NodaTime;
using NodaTime.Extensions;

namespace Energinet.DataHub.Measurements.Application.Persistence;

public class MeasurementResult(ExpandoObject raw)
{
    private readonly dynamic _raw = raw;

    public string MeteringPointId => _raw.metering_point_id;

    public string Unit => "KWh"; // _raw.unit;

    // public Instant ObservationTime => DateTimeOffsetExtensions.ToInstant(_raw.observation_time);
    public Instant ObservationTime => _raw.observation_time;

    public decimal Quantity => _raw.quantity;

    public string Quality => _raw.quality;
}

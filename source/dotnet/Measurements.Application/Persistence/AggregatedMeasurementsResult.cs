using System.Dynamic;
using NodaTime;

namespace Energinet.DataHub.Measurements.Application.Persistence;

public class AggregatedMeasurementsResult(ExpandoObject raw)
{
    private readonly dynamic _raw = raw;

    public Instant MinObservationTime => Instant.FromDateTimeOffset(_raw.min_observation_time);

    public Instant MaxObservationTime => Instant.FromDateTimeOffset(_raw.max_observation_time);

    public decimal Quantity => _raw.aggregated_quantity;

    public object[] Qualities => _raw.qualities; // TODO: maybe need to be object[]

    public long PointCount => _raw.point_count;
}

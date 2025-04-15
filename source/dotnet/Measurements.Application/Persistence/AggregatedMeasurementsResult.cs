using System.Dynamic;
using NodaTime;

namespace Energinet.DataHub.Measurements.Application.Persistence;

public class AggregatedMeasurementsResult(ExpandoObject raw)
{
    private readonly dynamic _raw = raw;

    public Instant MinObservationTime => Instant.FromDateTimeOffset(_raw.min_observation_time);

    public Instant MaxObservationTime => Instant.FromDateTimeOffset(_raw.max_observation_time);

    public decimal Quantity => _raw.aggregated_quantity;

    public object[] Qualities => _raw.qualities;

    public object[] Resolutions => _raw.resolutions;

    public object[] Units => _raw.units;

    public long PointCount => _raw.point_count;

    public long ObservationUpdates => _raw.observation_updates;
}

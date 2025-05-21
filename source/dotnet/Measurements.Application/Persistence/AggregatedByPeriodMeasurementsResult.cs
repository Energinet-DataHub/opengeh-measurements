using System.Dynamic;
using Energinet.DataHub.Measurements.Domain;
using NodaTime;

namespace Energinet.DataHub.Measurements.Application.Persistence;

public class AggregatedByPeriodMeasurementsResult(ExpandoObject raw)
{
    private readonly dynamic _raw = raw;

    public MeteringPoint MeteringPoint => new(_raw.metering_point_id);

    public Instant MinObservationTime => Instant.FromDateTimeOffset(_raw.min_observation_time);

    public Instant MaxObservationTime => Instant.FromDateTimeOffset(_raw.max_observation_time);

    public decimal? Quantity => _raw.aggregated_quantity;

    public object[] Qualities => _raw.qualities;

    public string Resolution => _raw.resolution;

    public string AggregationGroupKey => _raw.aggregation_group_key;
}

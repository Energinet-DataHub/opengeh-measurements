using Energinet.DataHub.Measurements.Application.Persistence;

namespace Energinet.DataHub.Measurements.Application.Responses;

public class MeasurementsAggregatedByPeriodResponse
{
    public MeasurementsAggregatedByPeriodResponse(List<AggregatedMeasurementsResult> aggregatedMeasurements)
    {
        AggregatedMeasurements = aggregatedMeasurements;
    }

    public List<AggregatedMeasurementsResult> AggregatedMeasurements { get; }

    public static MeasurementsAggregatedByPeriodResponse Create(IEnumerable<AggregatedMeasurementsResult> measurements)
    {
        return new MeasurementsAggregatedByPeriodResponse(measurements.ToList());
    }
}

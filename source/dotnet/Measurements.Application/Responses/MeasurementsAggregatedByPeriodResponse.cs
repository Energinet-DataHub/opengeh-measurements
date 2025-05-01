using Energinet.DataHub.Measurements.Application.Exceptions;
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
    var measurementAggregations = measurements.ToList();

    return measurementAggregations.Count <= 0
      ? throw new MeasurementsNotFoundDuringPeriodException()
      : new MeasurementsAggregatedByPeriodResponse(measurementAggregations);
  }
}

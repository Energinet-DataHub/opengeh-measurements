using Energinet.DataHub.Measurements.Abstractions.Api.Models;
using Energinet.DataHub.Measurements.Client.Extensions;
using Energinet.DataHub.Measurements.Client.Models;

namespace Energinet.DataHub.Measurements.Client.Factories;

public sealed class MeasurementAggregationFactory
{
    public MeasurementAggregation Create(AggregatedMeasurements aggregatedMeasurements)
    {
        var lowestAvailableQuality = aggregatedMeasurements.Qualities.Min();

        return new MeasurementAggregation(
            aggregatedMeasurements.MinObservationTime.ToLocalDate(),
            aggregatedMeasurements.Quantity,
            lowestAvailableQuality,
            SetMissingValuesForAggregation(aggregatedMeasurements));
    }

    private bool SetMissingValuesForAggregation(AggregatedMeasurements aggregatedMeasurements)
    {
        var timeSpan = aggregatedMeasurements.MaxObservationTime - aggregatedMeasurements.MinObservationTime;
        var hours = (int)timeSpan.TotalHours + 1;

        // All points for a day should have the same resolution
        var resolution = aggregatedMeasurements.Resolutions.Single();

        var expectedPointCount = resolution switch
        {
            Resolution.PT15M => hours * 4,
            Resolution.PT1H => hours,
            Resolution.P1D or Resolution.P1M or Resolution.P1Y => 1,
            _ => throw new ArgumentOutOfRangeException(resolution.ToString()),
        };

        return expectedPointCount - aggregatedMeasurements.PointCount != 0;
    }
}

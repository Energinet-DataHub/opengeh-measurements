using Energinet.DataHub.Measurements.Abstractions.Api.Models;
using Energinet.DataHub.Measurements.Client.Extensions;
using Energinet.DataHub.Measurements.Client.Models;
using NodaTime;

namespace Energinet.DataHub.Measurements.Client.Factories;

public sealed class MeasurementAggregationFactory
{
    public MeasurementAggregation Create(
        AggregatedMeasurements? aggregatedMeasurement,
        LocalDate date)
    {
        var lowestAvailableQuality = aggregatedMeasurement?.Qualities.Min();

        return new MeasurementAggregation(
            aggregatedMeasurement?.MinObservationTime.ToLocalDate() ?? date,
            aggregatedMeasurement?.Quantity ?? 0,
            lowestAvailableQuality ?? Quality.Missing,
            SetMissingValuesForAggregation(aggregatedMeasurement));
    }

    private bool SetMissingValuesForAggregation(AggregatedMeasurements? aggregatedMeasurement)
    {
        if (aggregatedMeasurement == null) return true;

        var timeSpan = aggregatedMeasurement.MaxObservationTime - aggregatedMeasurement.MinObservationTime;
        var hours = (int)timeSpan.TotalHours + 1;

        // All points for a doy should have the same resolution
        var resolution = aggregatedMeasurement.Resolutions.Single();

        var expectedPointCount = resolution switch
        {
            Resolution.PT15M => hours * 4,
            Resolution.PT1H => hours,
            Resolution.P1D or Resolution.P1M or Resolution.P1Y => 1,
            _ => throw new ArgumentOutOfRangeException(resolution.ToString()),
        };

        return expectedPointCount - aggregatedMeasurement.PointCount != 0;
    }
}

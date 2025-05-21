using System.ComponentModel;
using System.Text.Json.Serialization;
using Energinet.DataHub.Measurements.Application.Exceptions;
using Energinet.DataHub.Measurements.Application.Extensions;
using Energinet.DataHub.Measurements.Application.Persistence;
using Energinet.DataHub.Measurements.Application.Responses.EnumParsers;
using Energinet.DataHub.Measurements.Domain;

namespace Energinet.DataHub.Measurements.Application.Responses;

public class DeprecatedMeasurementsAggregatedByDateResponse
{
    // ReSharper disable once AutoPropertyCanBeMadeGetOnly.Global - used by System.Text.Json
    public List<DeprecatedMeasurementAggregationByDate> MeasurementAggregations { get; init; } = [];

    [JsonConstructor]
    [Browsable(false)]
    private DeprecatedMeasurementsAggregatedByDateResponse() { } // Needed by System.Text.Json to deserialize

    private DeprecatedMeasurementsAggregatedByDateResponse(List<DeprecatedMeasurementAggregationByDate> measurementAggregations)
    {
        MeasurementAggregations = measurementAggregations;
    }

    public static DeprecatedMeasurementsAggregatedByDateResponse Create(IEnumerable<AggregatedMeasurementsResult> measurements)
    {
        var measurementAggregations = measurements
            .Select(measurement =>
                new DeprecatedMeasurementAggregationByDate(
                    measurement.MinObservationTime.ToDateOnly(),
                    measurement.Quantity ?? 0,
                    FindMinimumQuality(measurement),
                    FindUnit(measurement),
                    FindContainsMissingValues(measurement),
                    FindContainsUpdatedValues(measurement)))
            .ToList();

        return measurementAggregations.Count <= 0
            ? throw new MeasurementsNotFoundException()
            : new DeprecatedMeasurementsAggregatedByDateResponse(measurementAggregations);
    }

    private static Quality FindMinimumQuality(AggregatedMeasurementsResult aggregatedMeasurementsResult)
    {
        return aggregatedMeasurementsResult.Qualities
            .Select(quality => QualityParser.ParseQuality((string)quality))
            .Min();
    }

    private static Unit FindUnit(AggregatedMeasurementsResult aggregatedMeasurementsResult)
    {
        return UnitParser.ParseUnit((string)aggregatedMeasurementsResult.Units.Single());
    }

    private static bool FindContainsMissingValues(AggregatedMeasurementsResult aggregatedMeasurements)
    {
        var resolution = ResolutionParser.ParseResolution((string)aggregatedMeasurements.Resolutions.Single());
        var quality = FindMinimumQuality(aggregatedMeasurements);
        var measurementsContainMissingQualities = quality <= Quality.Missing;

        var expectedPointCount = resolution.GetExpectedPointsForPeriod(aggregatedMeasurements.MinObservationTime, 1);

        return expectedPointCount - aggregatedMeasurements.PointCount != 0 ||
               measurementsContainMissingQualities;
    }

    private static bool FindContainsUpdatedValues(AggregatedMeasurementsResult aggregatedMeasurementsResult)
    {
        return aggregatedMeasurementsResult.ObservationUpdates > 1;
    }
}

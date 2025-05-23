using System.ComponentModel;
using System.Text.Json.Serialization;
using Energinet.DataHub.Measurements.Application.Exceptions;
using Energinet.DataHub.Measurements.Application.Extensions;
using Energinet.DataHub.Measurements.Application.Persistence;
using Energinet.DataHub.Measurements.Application.Responses.EnumParsers;
using Energinet.DataHub.Measurements.Domain;

namespace Energinet.DataHub.Measurements.Application.Responses;

[Obsolete("MeasurementsAggregatedByDateResponseV3 is deprecated. Use MeasurementsAggregatedByDateResponse instead.")]
public class MeasurementsAggregatedByDateResponseV3
{
    // ReSharper disable once AutoPropertyCanBeMadeGetOnly.Global - used by System.Text.Json
    public List<MeasurementAggregationByDateV3> MeasurementAggregations { get; init; } = [];

    [JsonConstructor]
    [Browsable(false)]
    private MeasurementsAggregatedByDateResponseV3() { } // Needed by System.Text.Json to deserialize

    private MeasurementsAggregatedByDateResponseV3(List<MeasurementAggregationByDateV3> measurementAggregations)
    {
        MeasurementAggregations = measurementAggregations;
    }

    public static MeasurementsAggregatedByDateResponseV3 Create(IEnumerable<AggregatedMeasurementsResult> measurements)
    {
        var measurementAggregations = measurements
            .Select(measurement =>
                new MeasurementAggregationByDateV3(
                    measurement.MinObservationTime.ToDateOnly(),
                    measurement.Quantity ?? 0,
                    FindMinimumQuality(measurement),
                    FindUnit(measurement),
                    FindIsMissingValues(measurement),
                    FindContainsUpdatedValues(measurement)))
            .ToList();

        return measurementAggregations.Count <= 0
            ? throw new MeasurementsNotFoundException()
            : new MeasurementsAggregatedByDateResponseV3(measurementAggregations);
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

    private static bool FindIsMissingValues(AggregatedMeasurementsResult aggregatedMeasurements)
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

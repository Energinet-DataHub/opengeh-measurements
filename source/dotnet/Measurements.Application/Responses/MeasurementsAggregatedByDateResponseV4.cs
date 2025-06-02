using System.ComponentModel;
using System.Text.Json.Serialization;
using Energinet.DataHub.Measurements.Application.Exceptions;
using Energinet.DataHub.Measurements.Application.Extensions;
using Energinet.DataHub.Measurements.Application.Persistence;
using Energinet.DataHub.Measurements.Application.Responses.EnumParsers;
using Energinet.DataHub.Measurements.Domain;

namespace Energinet.DataHub.Measurements.Application.Responses;

[Obsolete("Use MeasurementsAggregatedByDateResponse instead.")]
public class MeasurementsAggregatedByDateResponseV4
{
    // ReSharper disable once AutoPropertyCanBeMadeGetOnly.Global - used by System.Text.Json
    public IReadOnlyCollection<MeasurementAggregationByDateV4> MeasurementAggregations { get; init; } = [];

    [JsonConstructor]
    [Browsable(false)]
    private MeasurementsAggregatedByDateResponseV4() { } // Needed by System.Text.Json to deserialize

    private MeasurementsAggregatedByDateResponseV4(List<MeasurementAggregationByDateV4> measurementAggregations)
    {
        MeasurementAggregations = measurementAggregations;
    }

    public static MeasurementsAggregatedByDateResponseV4 Create(IEnumerable<AggregatedMeasurementsResult> measurements)
    {
        var measurementAggregations = measurements
            .Select(measurement =>
                new MeasurementAggregationByDateV4(
                    measurement.MinObservationTime.ToDateOnly(),
                    measurement.Quantity,
                    FindMinimumQuality(measurement),
                    FindUnit(measurement),
                    FindIsMissingValues(measurement),
                    FindContainsUpdatedValues(measurement)))
            .ToList();

        return measurementAggregations.Count <= 0
            ? throw new MeasurementsNotFoundException()
            : new MeasurementsAggregatedByDateResponseV4(measurementAggregations);
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

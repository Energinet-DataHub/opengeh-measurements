using System.ComponentModel;
using System.Text.Json.Serialization;
using Energinet.DataHub.Measurements.Application.Exceptions;
using Energinet.DataHub.Measurements.Application.Extensions;
using Energinet.DataHub.Measurements.Application.Persistence;
using Energinet.DataHub.Measurements.Application.Responses.EnumParsers;
using Energinet.DataHub.Measurements.Domain;

namespace Energinet.DataHub.Measurements.Application.Responses;

public class MeasurementsAggregatedByDateResponse
{
    // ReSharper disable once AutoPropertyCanBeMadeGetOnly.Global - used by System.Text.Json
    public IReadOnlyCollection<MeasurementAggregationByDate> MeasurementAggregations { get; init; } = [];

    [JsonConstructor]
    [Browsable(false)]
    private MeasurementsAggregatedByDateResponse() { } // Needed by System.Text.Json to deserialize

    private MeasurementsAggregatedByDateResponse(List<MeasurementAggregationByDate> measurementAggregations)
    {
        MeasurementAggregations = measurementAggregations;
    }

    public static MeasurementsAggregatedByDateResponse Create(IEnumerable<AggregatedMeasurementsResult> measurements)
    {
        var measurementAggregations = measurements
            .Select(measurement =>
                new MeasurementAggregationByDate(
                    measurement.MinObservationTime.ToDateOnly(),
                    measurement.Quantity,
                    SetQuality(measurement),
                    SetUnit(measurement),
                    SetMissingValuesForAggregation(measurement),
                    SetContainsUpdatedValues(measurement)))
            .ToList();

        return measurementAggregations.Count <= 0
            ? throw new MeasurementsNotFoundException()
            : new MeasurementsAggregatedByDateResponse(measurementAggregations);
    }

    private static Quality SetQuality(AggregatedMeasurementsResult aggregatedMeasurementsResult)
    {
        return aggregatedMeasurementsResult.Qualities
            .Select(quality => QualityParser.ParseQuality((string)quality))
            .Min();
    }

    private static Unit SetUnit(AggregatedMeasurementsResult aggregatedMeasurementsResult)
    {
        return UnitParser.ParseUnit((string)aggregatedMeasurementsResult.Units.Single());
    }

    private static bool SetMissingValuesForAggregation(AggregatedMeasurementsResult aggregatedMeasurements)
    {
        var resolution = ResolutionParser.ParseResolution((string)aggregatedMeasurements.Resolutions.Single());
        var lowestQuality = aggregatedMeasurements.Qualities.Min() ?? throw new InvalidOperationException("Could not parse quality");
        var quality = QualityParser.ParseQuality((string)lowestQuality);
        var measurementsContainMissingQualities = quality <= Quality.Missing;

        var expectedPointCount = resolution.GetExpectedPointsForPeriod(aggregatedMeasurements.MinObservationTime, 1);

        return expectedPointCount - aggregatedMeasurements.PointCount != 0 ||
               measurementsContainMissingQualities;
    }

    private static bool SetContainsUpdatedValues(AggregatedMeasurementsResult aggregatedMeasurementsResult)
    {
        return aggregatedMeasurementsResult.ObservationUpdates > 1;
    }
}

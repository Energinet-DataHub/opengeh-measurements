﻿using System.ComponentModel;
using System.Text.Json.Serialization;
using Energinet.DataHub.Measurements.Application.Exceptions;
using Energinet.DataHub.Measurements.Application.Extensions;
using Energinet.DataHub.Measurements.Application.Persistence;
using Energinet.DataHub.Measurements.Domain;

namespace Energinet.DataHub.Measurements.Application.Responses;

public class GetAggregatedMeasurementsResponse
{
    // ReSharper disable once AutoPropertyCanBeMadeGetOnly.Global - used by System.Text.Json
    public IReadOnlyCollection<MeasurementAggregation> MeasurementAggregations { get; init; } = [];

    [JsonConstructor]
    [Browsable(false)]
    private GetAggregatedMeasurementsResponse() { } // Needed by System.Text.Json to deserialize

    private GetAggregatedMeasurementsResponse(List<MeasurementAggregation> measurementAggregations)
    {
        MeasurementAggregations = measurementAggregations;
    }

    public static GetAggregatedMeasurementsResponse Create(IEnumerable<AggregatedMeasurementsResult> measurements)
    {
        var measurementAggregations = measurements
            .Select(measurement =>
                new MeasurementAggregation(
                    measurement.MinObservationTime.ToDateOnly(),
                    measurement.Quantity,
                    SetQuality(measurement),
                    SetUnit(measurement),
                    SetMissingValuesForAggregation(measurement),
                    SetContainsUpdatedValues(measurement)))
            .ToList();

        return measurementAggregations.Count <= 0
            ? throw new MeasurementsNotFoundDuringPeriodException()
            : new GetAggregatedMeasurementsResponse(measurementAggregations);
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
        var hours = GetHoursForAggregation(aggregatedMeasurements);

        // All points for a day should have the same resolution
        var resolution = ResolutionParser.ParseResolution((string)aggregatedMeasurements.Resolutions.Single());

        var expectedPointCount = GetExpectedPointCount(resolution, hours);

        return expectedPointCount - aggregatedMeasurements.PointCount != 0;
    }

    private static int GetHoursForAggregation(AggregatedMeasurementsResult aggregatedMeasurements)
    {
        var timeSpan = aggregatedMeasurements.MaxObservationTime - aggregatedMeasurements.MinObservationTime;
        var hours = (int)timeSpan.TotalHours + 1;
        return hours;
    }

    private static int GetExpectedPointCount(Resolution resolution, int hours)
    {
        var expectedPointCount = resolution switch
        {
            Resolution.QuarterHourly => hours * 4,
            Resolution.Hourly => hours,
            Resolution.Daily or Resolution.Monthly or Resolution.Yearly => 1,
            _ => throw new ArgumentOutOfRangeException(resolution.ToString()),
        };
        return expectedPointCount;
    }

    private static bool SetContainsUpdatedValues(AggregatedMeasurementsResult aggregatedMeasurementsResult)
    {
        return aggregatedMeasurementsResult.ObservationUpdates > 1;
    }
}

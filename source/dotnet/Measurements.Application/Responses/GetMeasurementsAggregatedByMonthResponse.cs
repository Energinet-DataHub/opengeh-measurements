using System.ComponentModel;
using System.Text.Json.Serialization;
using Energinet.DataHub.Measurements.Application.Exceptions;
using Energinet.DataHub.Measurements.Application.Extensions;
using Energinet.DataHub.Measurements.Application.Persistence;
using Energinet.DataHub.Measurements.Domain;
using NodaTime;

namespace Energinet.DataHub.Measurements.Application.Responses;

public class GetMeasurementsAggregatedByMonthResponse
{
    // ReSharper disable once AutoPropertyCanBeMadeGetOnly.Global - used by System.Text.Json
    public IReadOnlyCollection<MeasurementAggregationByMonth> MeasurementAggregations { get; init; } = [];

    [JsonConstructor]
    [Browsable(false)]
    private GetMeasurementsAggregatedByMonthResponse() { } // Needed by System.Text.Json to deserialize

    private GetMeasurementsAggregatedByMonthResponse(List<MeasurementAggregationByMonth> measurementAggregations)
    {
        MeasurementAggregations = measurementAggregations;
    }

    public static GetMeasurementsAggregatedByMonthResponse Create(IEnumerable<AggregatedMeasurementsResult> measurements)
    {
        var measurementAggregations = measurements
            .Select(measurement =>
                new MeasurementAggregationByMonth(
                    SetYearMonth(measurement),
                    measurement.Quantity,
                    SetQuality(measurement),
                    SetUnit(measurement),
                    SetMissingValuesForAggregation(measurement),
                    SetContainsUpdatedValues(measurement)))
            .ToList();

        return measurementAggregations.Count <= 0
            ? throw new MeasurementsNotFoundDuringPeriodException()
            : new GetMeasurementsAggregatedByMonthResponse(measurementAggregations);
    }

    private static YearMonth SetYearMonth(AggregatedMeasurementsResult measurement)
    {
        var dateOnly = measurement.MinObservationTime.ToDateOnly();
        return new YearMonth(dateOnly.Year, dateOnly.Month);
    }

    private static Quality SetQuality(AggregatedMeasurementsResult aggregatedMeasurementsResult)
    {
        return aggregatedMeasurementsResult.Qualities
            .Select(quality => QualityParser.ParseQuality((string)quality))
            .Min();
    }

    private static Unit SetUnit(AggregatedMeasurementsResult aggregatedMeasurementsResult)
    {
        return UnitParser.ParseUnit((string)aggregatedMeasurementsResult.Units.First()); // Todo
    }

    private static bool SetMissingValuesForAggregation(AggregatedMeasurementsResult aggregatedMeasurements)
    {
        var hours = GetHoursForAggregation(aggregatedMeasurements);

        // All points for a month should have the same resolution
        var resolution = ResolutionParser.ParseResolution((string)aggregatedMeasurements.Resolutions.First()); // Todo

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

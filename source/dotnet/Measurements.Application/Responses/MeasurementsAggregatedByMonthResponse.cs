using System.ComponentModel;
using System.Text.Json.Serialization;
using Energinet.DataHub.Measurements.Application.Exceptions;
using Energinet.DataHub.Measurements.Application.Extensions;
using Energinet.DataHub.Measurements.Application.Persistence;
using Energinet.DataHub.Measurements.Application.Responses.EnumParsers;
using Energinet.DataHub.Measurements.Domain;
using NodaTime;

namespace Energinet.DataHub.Measurements.Application.Responses;

public class MeasurementsAggregatedByMonthResponse
{
    // ReSharper disable once AutoPropertyCanBeMadeGetOnly.Global - used by System.Text.Json
    public IReadOnlyCollection<MeasurementAggregationByMonth> MeasurementAggregations { get; init; } = [];

    [JsonConstructor]
    [Browsable(false)]
    private MeasurementsAggregatedByMonthResponse() { } // Needed by System.Text.Json to deserialize

    private MeasurementsAggregatedByMonthResponse(List<MeasurementAggregationByMonth> measurementAggregations)
    {
        MeasurementAggregations = measurementAggregations;
    }

    public static MeasurementsAggregatedByMonthResponse Create(IEnumerable<AggregatedMeasurementsResult> measurements)
    {
        var measurementAggregations = measurements
            .Select(measurement =>
                new MeasurementAggregationByMonth(
                    FindYearMonth(measurement),
                    measurement.Quantity,
                    FindMinimumQuality(measurement),
                    SetUnit(measurement),
                    FindMissingValues(measurement)))
            .ToList();

        return measurementAggregations.Count <= 0
            ? throw new MeasurementsNotFoundException()
            : new MeasurementsAggregatedByMonthResponse(measurementAggregations);
    }

    private static YearMonth FindYearMonth(AggregatedMeasurementsResult measurement)
    {
        var dateOnly = measurement.MinObservationTime.ToDateOnly();
        return new YearMonth(dateOnly.Year, dateOnly.Month);
    }

    private static Quality FindMinimumQuality(AggregatedMeasurementsResult aggregatedMeasurementsResult)
    {
        return aggregatedMeasurementsResult.Qualities
            .Select(quality => QualityParser.ParseQuality((string)quality))
            .Min();
    }

    private static bool FindMissingValues(AggregatedMeasurementsResult aggregatedMeasurements)
    {
        var yearMonth = FindYearMonth(aggregatedMeasurements);
        var daysInMonth = yearMonth.Calendar.GetDaysInMonth(yearMonth.Year, yearMonth.Month);
        var resolution = ResolutionParser.ParseResolution((string)aggregatedMeasurements.Resolutions.Single());
        var expectedPointCount = resolution.GetExpectedPointsForPeriod(aggregatedMeasurements.MinObservationTime, daysInMonth);

        var quality = FindMinimumQuality(aggregatedMeasurements);
        var measurementsContainMissingQualities = quality <= Quality.Missing;

        return expectedPointCount - aggregatedMeasurements.PointCount != 0 ||
               measurementsContainMissingQualities;
    }

    private static Unit SetUnit(AggregatedMeasurementsResult aggregatedMeasurementsResult)
    {
        return UnitParser.ParseUnit((string)aggregatedMeasurementsResult.Units.First());
    }
}

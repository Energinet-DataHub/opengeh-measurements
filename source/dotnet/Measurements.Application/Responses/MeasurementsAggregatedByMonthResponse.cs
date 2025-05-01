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
                    SetYearMonth(measurement),
                    measurement.Quantity,
                    SetQuality(measurement),
                    SetUnit(measurement)))
            .ToList();

        return measurementAggregations.Count <= 0
            ? throw new MeasurementsNotFoundException()
            : new MeasurementsAggregatedByMonthResponse(measurementAggregations);
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
        return UnitParser.ParseUnit((string)aggregatedMeasurementsResult.Units.First());
    }
}

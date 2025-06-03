using System.ComponentModel;
using System.Text.Json.Serialization;
using Energinet.DataHub.Measurements.Application.Extensions;
using Energinet.DataHub.Measurements.Application.Persistence;
using Energinet.DataHub.Measurements.Application.Responses.EnumParsers;
using Energinet.DataHub.Measurements.Domain;
using NodaTime;

namespace Energinet.DataHub.Measurements.Application.Responses;

[Obsolete("Use MeasurementsAggregatedByMonthResponse instead.")]
public class MeasurementsAggregatedByMonthResponseV4
{
    // ReSharper disable once AutoPropertyCanBeMadeGetOnly.Global - used by System.Text.Json
    public IReadOnlyCollection<MeasurementAggregationByMonthV4> MeasurementAggregations { get; init; } = [];

    [JsonConstructor]
    [Browsable(false)]
    private MeasurementsAggregatedByMonthResponseV4() { } // Needed by System.Text.Json to deserialize

    private MeasurementsAggregatedByMonthResponseV4(List<MeasurementAggregationByMonthV4> measurementAggregations)
    {
        MeasurementAggregations = measurementAggregations;
    }

    public static MeasurementsAggregatedByMonthResponseV4? Create(IEnumerable<AggregatedMeasurementsResult> measurements)
    {
        var measurementAggregations = measurements
            .Select(measurement =>
                new MeasurementAggregationByMonthV4(
                    SetYearMonth(measurement),
                    measurement.Quantity,
                    SetQuality(measurement),
                    SetUnit(measurement)))
            .ToList();

        return measurementAggregations.Count <= 0
            ? null : new MeasurementsAggregatedByMonthResponseV4(measurementAggregations);
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

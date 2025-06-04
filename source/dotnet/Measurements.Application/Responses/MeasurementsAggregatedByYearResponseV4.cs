using System.ComponentModel;
using System.Text.Json.Serialization;
using Energinet.DataHub.Measurements.Application.Extensions;
using Energinet.DataHub.Measurements.Application.Persistence;
using Energinet.DataHub.Measurements.Application.Responses.EnumParsers;
using Energinet.DataHub.Measurements.Domain;

namespace Energinet.DataHub.Measurements.Application.Responses;

[Obsolete("Use MeasurementAggregationByYear instead.")]
public class MeasurementsAggregatedByYearResponseV4
{
    // ReSharper disable once AutoPropertyCanBeMadeGetOnly.Global - used by System.Text.Json
    public IReadOnlyCollection<MeasurementAggregationByYearV4> MeasurementAggregations { get; init; } = [];

    [JsonConstructor]
    [Browsable(false)]
    private MeasurementsAggregatedByYearResponseV4() { } // Needed by System.Text.Json to deserialize

    private MeasurementsAggregatedByYearResponseV4(List<MeasurementAggregationByYearV4> measurementAggregations)
    {
        MeasurementAggregations = measurementAggregations;
    }

    public static MeasurementsAggregatedByYearResponseV4 Create(IEnumerable<AggregatedMeasurementsResult> measurements)
    {
        var measurementAggregations = measurements
            .Select(measurement =>
                new MeasurementAggregationByYearV4(
                    SetYear(measurement),
                    measurement.Quantity,
                    SetQuality(measurement),
                    SetUnit(measurement)))
            .ToList();

        return new MeasurementsAggregatedByYearResponseV4(measurementAggregations);
    }

    private static int SetYear(AggregatedMeasurementsResult measurement)
    {
        return measurement.MinObservationTime.ToDateOnly().Year;
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

using System.ComponentModel;
using System.Text.Json.Serialization;
using Energinet.DataHub.Measurements.Application.Extensions;
using Energinet.DataHub.Measurements.Application.Persistence;
using Energinet.DataHub.Measurements.Application.Responses.EnumParsers;
using Energinet.DataHub.Measurements.Domain;

namespace Energinet.DataHub.Measurements.Application.Responses;

public class MeasurementsAggregatedByYearResponse
{
    // ReSharper disable once AutoPropertyCanBeMadeGetOnly.Global - used by System.Text.Json
    public IReadOnlyCollection<MeasurementAggregationByYear> MeasurementAggregations { get; init; } = [];

    [JsonConstructor]
    [Browsable(false)]
    private MeasurementsAggregatedByYearResponse() { } // Needed by System.Text.Json to deserialize

    private MeasurementsAggregatedByYearResponse(List<MeasurementAggregationByYear> measurementAggregations)
    {
        MeasurementAggregations = measurementAggregations;
    }

    public static MeasurementsAggregatedByYearResponse? Create(IEnumerable<AggregatedMeasurementsResult> measurements)
    {
        var measurementAggregations = measurements
            .Select(measurement =>
                new MeasurementAggregationByYear(
                    SetYear(measurement),
                    measurement.Quantity,
                    SetUnit(measurement)))
            .ToList();

        return measurementAggregations.Count <= 0
            ? null : new MeasurementsAggregatedByYearResponse(measurementAggregations);
    }

    private static int SetYear(AggregatedMeasurementsResult measurement)
    {
        return measurement.MinObservationTime.ToDateOnly().Year;
    }

    private static Unit SetUnit(AggregatedMeasurementsResult aggregatedMeasurementsResult)
    {
        // From a single metering point of view only one unit is allowed.
        // If unit should change then the metering point must be closed down and a new one created.
        return aggregatedMeasurementsResult.Units.Length != 1
            ? throw new InvalidOperationException("Aggregated measurements contains multiple units.")
            : UnitParser.ParseUnit((string)aggregatedMeasurementsResult.Units.Single());
    }
}

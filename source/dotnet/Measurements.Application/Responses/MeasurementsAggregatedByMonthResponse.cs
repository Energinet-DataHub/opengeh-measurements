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
                    SetUnit(measurement)))
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

    private static Unit SetUnit(AggregatedMeasurementsResult aggregatedMeasurementsResult)
    {
        // From a single metering point of view only one unit is allowed.
        // If unit should change then the metering point must be closed down and a new one created.
        return aggregatedMeasurementsResult.Units.Length != 1
            ? throw new InvalidOperationException("Aggregated measurements contains multiple units.")
            : UnitParser.ParseUnit((string)aggregatedMeasurementsResult.Units.Single());
    }
}

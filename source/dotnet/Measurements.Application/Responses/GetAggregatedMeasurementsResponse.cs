using System.ComponentModel;
using System.Text.Json.Serialization;
using Energinet.DataHub.Measurements.Application.Exceptions;
using Energinet.DataHub.Measurements.Application.Persistence;
using Energinet.DataHub.Measurements.Domain;

namespace Energinet.DataHub.Measurements.Application.Responses;

public class GetAggregatedMeasurementsResponse
{
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
                    measurement.MinObservationTime,
                    measurement.MaxObservationTime,
                    measurement.Quantity,
                    measurement.Qualities.Select(quality => QualityParser.ParseQuality((string)quality)),
                    measurement.PointCount))
            .ToList();

        return measurementAggregations.Count <= 0
            ? throw new MeasurementsNotFoundDuringPeriodException()
            : new GetAggregatedMeasurementsResponse(measurementAggregations);
    }
}

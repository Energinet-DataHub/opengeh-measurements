using System.ComponentModel;
using System.Text.Json.Serialization;
using Energinet.DataHub.Measurements.Application.Exceptions;
using Energinet.DataHub.Measurements.Application.Extensions;
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
                    measurement.MinObservationTime.ToDateOnly(),
                    measurement.Quantity,
                    measurement.Qualities.Select(quality => QualityParser.ParseQuality((string)quality)).Min(),
                    SetMissingValuesForAggregation(measurement)))
            .ToList();

        return measurementAggregations.Count <= 0
            ? throw new MeasurementsNotFoundDuringPeriodException()
            : new GetAggregatedMeasurementsResponse(measurementAggregations);
    }

    private static bool SetMissingValuesForAggregation(AggregatedMeasurementsResult aggregatedMeasurements)
    {
        var timeSpan = aggregatedMeasurements.MaxObservationTime - aggregatedMeasurements.MinObservationTime;
        var hours = (int)timeSpan.TotalHours + 1;

        // All points for a day should have the same resolution
        var resolution = ResolutionParser.ParseResolution((string)aggregatedMeasurements.Resolutions.Single());

        var expectedPointCount = resolution switch
        {
            Resolution.PT15M => hours * 4,
            Resolution.PT1H => hours,
            Resolution.P1D or Resolution.P1M or Resolution.P1Y => 1,
            _ => throw new ArgumentOutOfRangeException(resolution.ToString()),
        };

        return expectedPointCount - aggregatedMeasurements.PointCount != 0;
    }
}

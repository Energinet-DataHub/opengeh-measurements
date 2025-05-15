using System.ComponentModel;
using System.Text.Json.Serialization;
using Energinet.DataHub.Measurements.Application.Exceptions;
using Energinet.DataHub.Measurements.Application.Persistence;
using Energinet.DataHub.Measurements.Application.Responses.EnumParsers;
using Energinet.DataHub.Measurements.Domain;
using NodaTime;

namespace Energinet.DataHub.Measurements.Application.Responses;

public class MeasurementsAggregatedByPeriodResponse
{
    // ReSharper disable once AutoPropertyCanBeMadeGetOnly.Global - used by System.Text.Json
    public IReadOnlyCollection<MeasurementAggregationByPeriod> MeasurementAggregations { get; init; } = [];

    [JsonConstructor]
    [Browsable(false)]
    private MeasurementsAggregatedByPeriodResponse() { } // Needed by System.Text.Json to deserialize

    private MeasurementsAggregatedByPeriodResponse(List<MeasurementAggregationByPeriod> measurementAggregations)
    {
        MeasurementAggregations = measurementAggregations;
    }

    public static MeasurementsAggregatedByPeriodResponse Create(IEnumerable<AggregatedByPeriodMeasurementsResult> measurements)
    {
        var measurementsAggregatedDtos = new Dictionary<string, MeasurementAggregationByPeriod>();
        var pointAggregationGroups = new Dictionary<string, PointAggregationGroup>();

        foreach (var measurement in measurements)
        {
            var pointAggregation = new PointAggregation(
                measurement.MinObservationTime,
                measurement.MaxObservationTime,
                measurement.Quantity,
                SetQuality(measurement));

            var aggregationGroupKey =
                new AggregationGroupCompositeKey(measurement.MeteringPoint, measurement.AggregationGroupKey);
            var resolution = ResolutionParser.ParseResolution(measurement.Resolution);
            var pointAggregationGroup = GetOrCreatePointAggregationGroup(
                pointAggregationGroups,
                aggregationGroupKey.Key,
                measurement.MinObservationTime,
                measurement.MaxObservationTime,
                resolution);

            pointAggregationGroup.PointAggregations.Add(pointAggregation);

            var measurementsAggregatedByPeriod = CreateMeasurementsAggregatedByPeriodIfNotExists(
                measurementsAggregatedDtos,
                measurement);
            measurementsAggregatedByPeriod.PointAggregationGroups[aggregationGroupKey.Key] = pointAggregationGroup;
        }

        var measurementAggregations = measurementsAggregatedDtos
            .Select(measurement => measurement.Value)
            .ToList();

        return measurementAggregations.Count <= 0
            ? throw new MeasurementsNotFoundException()
            : new MeasurementsAggregatedByPeriodResponse(measurementAggregations);
    }

    private static MeasurementAggregationByPeriod CreateMeasurementsAggregatedByPeriodIfNotExists(
        Dictionary<string, MeasurementAggregationByPeriod> measurementsAggregatedDtos,
        AggregatedByPeriodMeasurementsResult measurement)
    {
        if (measurementsAggregatedDtos.TryGetValue(measurement.MeteringPoint.Id, out var existing))
            return existing;

        var measurementAggregationByPeriod = new MeasurementAggregationByPeriod(
            measurement.MeteringPoint,
            []);
        measurementsAggregatedDtos.Add(measurement.MeteringPoint.Id, measurementAggregationByPeriod);
        return measurementAggregationByPeriod;
    }

    private static PointAggregationGroup GetOrCreatePointAggregationGroup(
        Dictionary<string, PointAggregationGroup> pointAggregationGroups,
        string aggregationGroupKey,
        Instant minObservationTime,
        Instant maxObservationTime,
        Resolution resolution)
    {
        if (pointAggregationGroups.TryGetValue(aggregationGroupKey, out var pointAggregationGroup))
        {
            return pointAggregationGroup;
        }

        pointAggregationGroup = new PointAggregationGroup(
            minObservationTime,
            maxObservationTime,
            resolution,
            []);
        pointAggregationGroups[aggregationGroupKey] = pointAggregationGroup;
        return pointAggregationGroup;
    }

    private static Quality SetQuality(AggregatedByPeriodMeasurementsResult aggregatedByPeriodMeasurementsResult)
    {
        return aggregatedByPeriodMeasurementsResult.Qualities
            .Select(quality => QualityParser.ParseQuality((string)quality))
            .Min();
    }
}

namespace Energinet.DataHub.Measurements.Domain;

public record MeasurementAggregationByPeriod(MeteringPoint MeteringPoint, Dictionary<string, PointAggregationGroup> PointAggregationGroups);

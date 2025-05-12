namespace Energinet.DataHub.Measurements.Abstractions.Api.Models;

public record PointAggregationGroup(
    long MinObservationTime,
    long MaxObservationTime,
    Resolution Resolution,
    List<PointAggregation> PointAggregations);

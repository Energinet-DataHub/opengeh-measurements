namespace Energinet.DataHub.Measurements.Domain;

public record AggregationGroupCompositeKey(MeteringPoint MeteringPoint, string AggregateGroup, Resolution Resolution)
{
    public string Key => $"{MeteringPoint.Id}_{AggregateGroup}_{Resolution}";
}

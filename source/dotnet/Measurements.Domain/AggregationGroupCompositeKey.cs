namespace Energinet.DataHub.Measurements.Domain;

public record AggregationGroupCompositeKey(MeteringPoint MeteringPoint, string AggregateGroup)
{
    public string Key => $"{MeteringPoint.Id}_{AggregateGroup}";
}

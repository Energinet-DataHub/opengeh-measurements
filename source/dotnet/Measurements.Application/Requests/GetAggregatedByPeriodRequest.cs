using System.ComponentModel.DataAnnotations;
using Energinet.DataHub.Measurements.Domain;

public record GetAggregatedByPeriodRequest(
    [property: Required] string MeteringPointIds,
    [property: Required] DateTimeOffset DateFrom,
    [property: Required] DateTimeOffset DateTo,
    [property: Required] Aggregation Aggregation)
{
  public List<string> GetMeteringPointIds() =>
      MeteringPointIds.Trim().Split(',').ToList();

  public override string ToString() =>
      $"MeteringPointIds: {MeteringPointIds}, DateFrom (UTC): {DateFrom:O}, DateTo (UTC): {DateTo:O}, Aggregation: {Aggregation}";
}

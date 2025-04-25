using System.ComponentModel.DataAnnotations;

namespace Energinet.DataHub.Measurements.Application.Requests;

public record GetAggregatedByMonthRequest(string MeteringPointId, [Range(-9998, 9999)] int Year);

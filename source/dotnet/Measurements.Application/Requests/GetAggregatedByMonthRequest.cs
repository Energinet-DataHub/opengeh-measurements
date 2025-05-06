using System.ComponentModel.DataAnnotations;

namespace Energinet.DataHub.Measurements.Application.Requests;

public record GetAggregatedByMonthRequest(
    [Required] [StringLength(18, ErrorMessage = "MeteringPointId must be a valid GLN number.")] string MeteringPointId,
    [Required] [Range(1970, 9999)] int Year);

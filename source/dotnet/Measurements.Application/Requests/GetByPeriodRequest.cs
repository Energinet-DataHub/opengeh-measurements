using System.ComponentModel.DataAnnotations;

namespace Energinet.DataHub.Measurements.Application.Requests;

public record GetByPeriodRequest(
    [Required] [StringLength(13, ErrorMessage = "MeteringPointId must be a valid GLN number.")] string MeteringPointId,
    [Required] DateTimeOffset StartDate,
    [Required] DateTimeOffset EndDate);

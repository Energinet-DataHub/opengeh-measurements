using System.ComponentModel.DataAnnotations;

namespace Energinet.DataHub.Measurements.Application.Requests;

public record GetAggregatedByYearRequest(
    [Required] [StringLength(18, ErrorMessage = "MeteringPointId must be a valid GLN number.")] string MeteringPointId);

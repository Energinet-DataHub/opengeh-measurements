﻿using System.ComponentModel.DataAnnotations;

namespace Energinet.DataHub.Measurements.Application.Requests;

public record GetAggregatedByDateRequest(
    [Required] [StringLength(18, ErrorMessage = "MeteringPointId must be a valid GLN number.")] string MeteringPointId,
    [Required] [Range(1970, 9999)] int Year,
    [Required] [Range(1, 12)] int Month);

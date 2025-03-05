using Energinet.DataHub.Measurements.Application.Dtos;

namespace Energinet.DataHub.Measurements.Application.Responses;

public record GetMeasurementResponse(
    string MeteringPointId,
    string Unit,
    IEnumerable<PointDto> Points);

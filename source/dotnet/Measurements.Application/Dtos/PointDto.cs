using NodaTime;

namespace Energinet.DataHub.Measurements.Application.Dtos;

public record PointDto(
    Instant ObservationTime,
    decimal Quantity,
    string Quality);

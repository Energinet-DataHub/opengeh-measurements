using NodaTime;

namespace Energinet.DataHub.Measurements.Application.Requests;

public record GetMeasurementRequest(string MeteringPointId, Instant StartDate, Instant EndDate);

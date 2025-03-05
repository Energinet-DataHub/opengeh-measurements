using NodaTime;

namespace Energinet.DataHub.Measurements.WebApi.Requests;

public record GetMeasurementRequest(string MeteringPointId, Instant StartDate, Instant EndDate);

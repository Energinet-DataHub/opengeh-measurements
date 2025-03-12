namespace Energinet.DataHub.Measurements.Application.Requests;

public record GetMeasurementRequest(string MeteringPointId, DateTimeOffset StartDate, DateTimeOffset EndDate);

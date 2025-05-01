namespace Energinet.DataHub.Measurements.Application.Requests;

public record GetByPeriodRequest(string MeteringPointId, DateTimeOffset StartDate, DateTimeOffset EndDate);

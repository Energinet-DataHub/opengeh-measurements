namespace Energinet.DataHub.Measurements.Application.Requests;

public record GetAggregatedMeasurementsRequest(string MeteringPointIds, DateTime DateFrom, DateTime DateTo);

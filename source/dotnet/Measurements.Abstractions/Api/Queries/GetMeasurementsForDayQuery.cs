namespace Energinet.DataHub.Measurements.Abstractions.Api.Queries;

/// <summary>
/// Query for getting all measurements for a metering point on a specific day.
/// </summary>
public sealed record GetMeasurementsForDayQuery(string MeteringPointId, DateTimeOffset Date);

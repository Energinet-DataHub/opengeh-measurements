using NodaTime;

namespace Energinet.DataHub.Measurements.Abstractions.Api.Queries;

/// <summary>
/// Query for getting all measurements for a metering point on a specific day in UTC.
/// </summary>
public sealed record GetMeasurementsForDayQuery(string MeteringPointId, LocalDate Date);

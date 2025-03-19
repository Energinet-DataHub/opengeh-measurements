using NodaTime;

namespace Energinet.DataHub.Measurements.Abstractions.Api.Queries;

/// <summary>
/// Query for getting all measurements for a metering point in a period specified by from and to dates.
/// </summary>
public sealed record GetMeasurementsForPeriodQuery(string MeteringPointId, LocalDate FromDate, LocalDate ToDate);

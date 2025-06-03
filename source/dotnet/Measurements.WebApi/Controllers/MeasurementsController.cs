using Asp.Versioning;
using Energinet.DataHub.Measurements.Application.Handlers;
using Energinet.DataHub.Measurements.Application.Requests;
using Energinet.DataHub.Measurements.WebApi.Constants;
using Energinet.DataHub.Measurements.WebApi.Extensions;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace Energinet.DataHub.Measurements.WebApi.Controllers;

[ApiController]
[Authorize(AuthenticationSchemes = $"{AuthenticationSchemas.Default},{AuthenticationSchemas.B2C}")]
[ApiVersion(4.0)]
[ApiVersion(5.0)]
[Route("v{v:apiVersion}/measurements")]
public class MeasurementsController(
    IMeasurementsHandler measurementsHandler, ILogger<MeasurementsController> logger)
    : ControllerBase
{
    [MapToApiVersion(4.0)]
    [MapToApiVersion(5.0)]
    [HttpGet("forPeriod")]
    public async Task<IActionResult> GetByPeriodAsync([FromQuery] GetByPeriodRequest request)
    {
        var measurement = await measurementsHandler.GetByPeriodAsync(request);

        if (measurement is not null) return Ok(measurement);

        logger.LogInformation(
            "Measurements not found for metering point id {MeteringPointId} from {StartDate} to {EndDate}",
            request.MeteringPointId.Sanitize(),
            request.StartDate,
            request.EndDate);

        return NotFound("No measurements found for the specified period.");
    }

    [MapToApiVersion(4.0)]
    [MapToApiVersion(5.0)]
    [HttpGet("currentForPeriod")]
    public Task<IActionResult> GetCurrentByPeriodAsync([FromQuery] GetByPeriodRequest request)
    {
        return Task.FromResult<IActionResult>(Accepted("This endpoint is not implemented yet."));
    }

    [MapToApiVersion(4.0)]
    [HttpGet("aggregatedByDate")]
    [Obsolete("Obsolete endpoint, use GetAggregatedByDateAsync instead.")]
    public async Task<IActionResult> GetAggregatedByDateAsyncV4([FromQuery] GetAggregatedByDateRequest request)
    {
        var aggregatedByMonth = await measurementsHandler.GetAggregatedByDateAsyncV4(request);

        if (aggregatedByMonth is not null) return Ok(aggregatedByMonth);

        logger.LogInformation(
            "Aggregation by year and month not found for metering point id {MeteringPointId} during {Year}-{Month}",
            request.MeteringPointId.Sanitize(),
            request.Year,
            request.Month);

        return NotFound("No aggregated measurements found for the specified date.");
    }

    [MapToApiVersion(5.0)]
    [HttpGet("aggregatedByDate")]
    public async Task<IActionResult> GetAggregatedByDateAsync([FromQuery] GetAggregatedByDateRequest request)
    {
        var aggregatedByMonth = await measurementsHandler.GetAggregatedByDateAsync(request);

        if (aggregatedByMonth is not null) return Ok(aggregatedByMonth);

        logger.LogInformation(
            "Aggregation by year and month not found for metering point id {MeteringPointId} during {Year}-{Month}",
            request.MeteringPointId.Sanitize(),
            request.Year,
            request.Month);

        return NotFound("No aggregated measurements found for the specified date.");
    }

    [MapToApiVersion(4.0)]
    [HttpGet("aggregatedByMonth")]
    [Obsolete("Obsolete. Use GetAggregatedByMonthAsync instead.")]
    public async Task<IActionResult> GetAggregatedByMonthAsyncV4([FromQuery] GetAggregatedByMonthRequest request)
    {
        var aggregatedByYear = await measurementsHandler.GetAggregatedByMonthAsyncV4(request);

        if (aggregatedByYear is not null) return Ok(aggregatedByYear);

        logger.LogInformation(
            "Aggregation by year not found for metering point id {MeteringPointId} during {Year}",
            request.MeteringPointId.Sanitize(),
            request.Year);

        return NotFound("No aggregated measurements found for the specified month.");
    }

    [MapToApiVersion(5.0)]
    [HttpGet("aggregatedByMonth")]
    public async Task<IActionResult> GetAggregatedByMonthAsync([FromQuery] GetAggregatedByMonthRequest request)
    {
        var aggregatedByYear = await measurementsHandler.GetAggregatedByMonthAsync(request);

        if (aggregatedByYear is not null) return Ok(aggregatedByYear);

        logger.LogInformation(
            "Aggregation by year not found for metering point id {MeteringPointId} during {Year}",
            request.MeteringPointId.Sanitize(),
            request.Year);

        return NotFound("No aggregated measurements found for the specified month.");
    }

    [MapToApiVersion(4.0)]
    [HttpGet("aggregatedByYear")]
    [Obsolete("Use GetAggregatedByYearAsync instead.")]
    public async Task<IActionResult> GetAggregatedByYearAsyncV4([FromQuery] GetAggregatedByYearRequest request)
    {
        var aggregatedByYear = await measurementsHandler.GetAggregatedByYearAsyncV4(request);

        if (aggregatedByYear is not null) return Ok(aggregatedByYear);

        logger.LogInformation(
            "Aggregation by year not found for metering point id {MeteringPointId} for all years",
            request.MeteringPointId.Sanitize());

        return NotFound("No aggregated measurements found for the specified year.");
    }

    [MapToApiVersion(5.0)]
    [HttpGet("aggregatedByYear")]
    public async Task<IActionResult> GetAggregatedByYearAsync([FromQuery] GetAggregatedByYearRequest request)
    {
        var aggregatedByYear = await measurementsHandler.GetAggregatedByYearAsync(request);

        if (aggregatedByYear is not null) return Ok(aggregatedByYear);

        logger.LogInformation(
            "Aggregation by year not found for metering point id {MeteringPointId} for all years",
            request.MeteringPointId.Sanitize());

        return NotFound("No aggregated measurements found for the specified year.");
    }

    [MapToApiVersion(4.0)]
    [MapToApiVersion(5.0)]
    [HttpGet("aggregatedByPeriod")]
    public async Task<IActionResult> GetAggregatedByPeriodAsync([FromQuery] GetAggregatedByPeriodRequest request)
    {
        var aggregatedByPeriod = await measurementsHandler.GetAggregatedByPeriodAsync(request);

        if (aggregatedByPeriod is not null) return Ok(aggregatedByPeriod);

        logger.LogInformation(
            "Aggregated measurements not found for metering point ids {MeteringPointIds} from {StartDate} to {EndDate} with aggregation {Aggregation}",
            request.MeteringPointIds.Sanitize(),
            request.From,
            request.To,
            request.Aggregation);

        return NotFound("No aggregated measurements found for the specified period.");
    }
}

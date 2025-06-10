using Asp.Versioning;
using Energinet.DataHub.Measurements.Application.Handlers;
using Energinet.DataHub.Measurements.Application.Requests;
using Energinet.DataHub.Measurements.Infrastructure.Serialization;
using Energinet.DataHub.Measurements.WebApi.Constants;
using Energinet.DataHub.Measurements.WebApi.Extensions;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace Energinet.DataHub.Measurements.WebApi.Controllers;

[ApiController]
[Authorize(AuthenticationSchemes = $"{JwtBearerDefaults.AuthenticationScheme},{AuthenticationSchemas.B2C}")]
[ApiVersion(4.0)]
[ApiVersion(5.0)]
[Route("v{v:apiVersion}/measurements")]
public class MeasurementsController(
    IMeasurementsHandler measurementsHandler, ILogger<MeasurementsController> logger, IJsonSerializer jsonSerializer)
    : ControllerBase
{
    [MapToApiVersion(4.0)]
    [MapToApiVersion(5.0)]
    [HttpGet("forPeriod")]
    public async Task<IActionResult> GetByPeriodAsync([FromQuery] GetByPeriodRequest request)
    {
        var measurement = await measurementsHandler.GetByPeriodAsync(request);

        if (measurement.Points.Count > 0)
        {
            return Ok(measurement);
        }

        logger.LogInformation(
            "Measurements not found for metering point id {MeteringPointId} from {StartDate} to {EndDate}",
            request.MeteringPointId.ToSanitizedString(),
            request.StartDate.ToSanitizedString(),
            request.EndDate.ToSanitizedString());

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

        if (aggregatedByMonth.MeasurementAggregations.Count > 0)
        {
            var result = jsonSerializer.Serialize(aggregatedByMonth);
            return Ok(result);
        }

        logger.LogInformation(
            "Aggregation by year and month not found for metering point id {MeteringPointId} during {Year}-{Month}",
            request.MeteringPointId.ToSanitizedString(),
            request.Year,
            request.Month);

        return NotFound("No aggregated measurements found for the specified date.");
    }

    [MapToApiVersion(5.0)]
    [HttpGet("aggregatedByDate")]
    public async Task<IActionResult> GetAggregatedByDateAsync([FromQuery] GetAggregatedByDateRequest request)
    {
        var aggregatedByMonth = await measurementsHandler.GetAggregatedByDateAsync(request);

        if (aggregatedByMonth.MeasurementAggregations.Count > 0)
        {
            var result = jsonSerializer.Serialize(aggregatedByMonth);
            return Ok(result);
        }

        logger.LogInformation(
            "Aggregation by year and month not found for metering point id {MeteringPointId} during {Year}-{Month}",
            request.MeteringPointId.ToSanitizedString(),
            request.Year,
            request.Month);

        return NotFound("No aggregated measurements found for the specified year and month.");
    }

    [MapToApiVersion(4.0)]
    [HttpGet("aggregatedByMonth")]
    [Obsolete("Obsolete. Use GetAggregatedByMonthAsync instead.")]
    public async Task<IActionResult> GetAggregatedByMonthAsyncV4([FromQuery] GetAggregatedByMonthRequest request)
    {
        var aggregatedByYear = await measurementsHandler.GetAggregatedByMonthAsyncV4(request);

        if (aggregatedByYear.MeasurementAggregations.Count > 0)
        {
            var result = jsonSerializer.Serialize(aggregatedByYear);
            return Ok(result);
        }

        logger.LogInformation(
            "Aggregation by year not found for metering point id {MeteringPointId} during {Year}",
            request.MeteringPointId.ToSanitizedString(),
            request.Year);

        return NotFound("No aggregated measurements found for the specified year.");
    }

    [MapToApiVersion(5.0)]
    [HttpGet("aggregatedByMonth")]
    public async Task<IActionResult> GetAggregatedByMonthAsync([FromQuery] GetAggregatedByMonthRequest request)
    {
        var aggregatedByYear = await measurementsHandler.GetAggregatedByMonthAsync(request);

        if (aggregatedByYear.MeasurementAggregations.Count > 0)
        {
            var result = jsonSerializer.Serialize(aggregatedByYear);
            return Ok(result);
        }

        logger.LogInformation(
            "Aggregation by year not found for metering point id {MeteringPointId} during {Year}",
            request.MeteringPointId.ToSanitizedString(),
            request.Year);

        return NotFound("No aggregated measurements found for the specified year.");
    }

    [MapToApiVersion(4.0)]
    [HttpGet("aggregatedByYear")]
    [Obsolete("Use GetAggregatedByYearAsync instead.")]
    public async Task<IActionResult> GetAggregatedByYearAsyncV4([FromQuery] GetAggregatedByYearRequest request)
    {
        var aggregatedByYear = await measurementsHandler.GetAggregatedByYearAsyncV4(request);

        if (aggregatedByYear.MeasurementAggregations.Count > 0)
        {
            var result = jsonSerializer.Serialize(aggregatedByYear);
            return Ok(result);
        }

        logger.LogInformation(
            "Aggregation by year not found for metering point id {MeteringPointId} for any year",
            request.MeteringPointId.ToSanitizedString());

        return NotFound("No aggregated measurements found.");
    }

    [MapToApiVersion(5.0)]
    [HttpGet("aggregatedByYear")]
    public async Task<IActionResult> GetAggregatedByYearAsync([FromQuery] GetAggregatedByYearRequest request)
    {
        var aggregatedByYear = await measurementsHandler.GetAggregatedByYearAsync(request);

        if (aggregatedByYear.MeasurementAggregations.Count > 0)
        {
            var result = jsonSerializer.Serialize(aggregatedByYear);
            return Ok(result);
        }

        logger.LogInformation(
            "Aggregation by year not found for metering point id {MeteringPointId} for any year",
            request.MeteringPointId.ToSanitizedString());

        return NotFound("No aggregated measurements found.");
    }

    [MapToApiVersion(4.0)]
    [MapToApiVersion(5.0)]
    [HttpGet("aggregatedByPeriod")]
    public async Task<IActionResult> GetAggregatedByPeriodAsync([FromQuery] GetAggregatedByPeriodRequest request)
    {
        var aggregatedByPeriod = await measurementsHandler.GetAggregatedByPeriodAsync(request);

        if (aggregatedByPeriod.MeasurementAggregations.Count > 0)
        {
            var result = jsonSerializer.Serialize(aggregatedByPeriod);
            return Ok(result);
        }

        logger.LogInformation(
            "Aggregated measurements not found for metering point ids {MeteringPointIds} from {StartDate} to {EndDate} with aggregation {Aggregation}",
            request.MeteringPointIds.ToSanitizedString(),
            request.From.ToSanitizedString(),
            request.To.ToSanitizedString(),
            request.Aggregation);

        return NotFound("No aggregated measurements found for the specified period.");
    }
}

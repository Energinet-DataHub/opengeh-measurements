using Asp.Versioning;
using Energinet.DataHub.Measurements.Application.Exceptions;
using Energinet.DataHub.Measurements.Application.Handlers;
using Energinet.DataHub.Measurements.Application.Requests;
using Energinet.DataHub.Measurements.Infrastructure.Serialization;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace Energinet.DataHub.Measurements.WebApi.Controllers;

[ApiVersion(1.0, Deprecated = true)]
[ApiVersion(2.0)]
[ApiController]
[Authorize]
public class MeasurementsController(IMeasurementsHandler measurementsHandler, ILogger<MeasurementsController> logger)
    : ControllerBase
{
    [MapToApiVersion(1.0)]
    [HttpGet("forPeriod")]
    public async Task<IActionResult> GetByPeriodAsyncV1([FromQuery] GetByPeriodRequest request)
    {
        try
        {
            var measurement = await measurementsHandler.GetByPeriodAsyncV1(request);
            var result = new JsonSerializer().Serialize(measurement);

            return Ok(result);
        }
        catch (MeasurementsNotFoundDuringPeriodException e)
        {
            return NotFound(e.Message);
        }
        catch (Exception exception)
        {
            logger.LogError(exception, "Could not get requested measurement");

            return StatusCode(StatusCodes.Status500InternalServerError, exception.Message);
        }
    }

    [MapToApiVersion(2.0)]
    [HttpGet("forPeriod")]
    public async Task<IActionResult> GetByPeriodAsync([FromQuery] GetByPeriodRequest request)
    {
        try
        {
            var measurement = await measurementsHandler.GetByPeriodAsync(request);
            var result = new JsonSerializer().Serialize(measurement);

            return Ok(result);
        }
        catch (MeasurementsNotFoundDuringPeriodException e)
        {
            return NotFound(e.Message);
        }
        catch (Exception exception)
        {
            logger.LogError(exception, "Could not get requested measurement");

            return StatusCode(StatusCodes.Status500InternalServerError, exception.Message);
        }
    }

    [MapToApiVersion(1.0)]
    [MapToApiVersion(2.0)]
    [HttpGet("aggregatedByMonth")]
    public async Task<IActionResult> GetAggregatedByMonthAsync([FromQuery] GetAggregatedByMonthRequest request)
    {
        try
        {
            var aggregatedByMonth = await measurementsHandler.GetAggregatedByMonthAsync(request);
            var result = new JsonSerializer().Serialize(aggregatedByMonth);

            return Ok(result);
        }
        catch (MeasurementsNotFoundDuringPeriodException e)
        {
            return NotFound(e.Message);
        }
    }

    [MapToApiVersion(1.0)]
    [HttpGet("aggregatedByYear")]
    public async Task<IActionResult> GetAggregatedByYearAsync([FromQuery] GetAggregatedByYearRequest request)
    {
        try
        {
            var aggregatedByYear = await measurementsHandler.GetAggregatedByYearAsync(request);
            var result = new JsonSerializer().Serialize(aggregatedByYear);

            return Ok(result);
        }
        catch (MeasurementsNotFoundDuringPeriodException e)
        {
            return NotFound(e.Message);
        }
    }
}

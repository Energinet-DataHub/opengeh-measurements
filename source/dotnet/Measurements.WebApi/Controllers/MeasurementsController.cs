using Asp.Versioning;
using Energinet.DataHub.Measurements.Application.Exceptions;
using Energinet.DataHub.Measurements.Application.Handlers;
using Energinet.DataHub.Measurements.Application.Requests;
using Energinet.DataHub.Measurements.Infrastructure.Serialization;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace Energinet.DataHub.Measurements.WebApi.Controllers;

[ApiController]
[Authorize]
[ApiVersion(2.0)]
[Route("v{v:apiVersion}/measurements")]
public class MeasurementsController(
    IMeasurementsHandler measurementsHandler, ILogger<MeasurementsController> logger, IJsonSerializer jsonSerializer)
    : ControllerBase
{
    [MapToApiVersion(2.0)]
    [HttpGet("forPeriod")]
    public async Task<IActionResult> GetByPeriodAsync([FromQuery] GetByPeriodRequest request)
    {
        try
        {
            var measurement = await measurementsHandler.GetByPeriodAsync(request);
            var result = jsonSerializer.Serialize(measurement);

            return Ok(result);
        }
        catch (MeasurementsNotFoundDuringPeriodException e)
        {
            return NotFound(e.Message);
        }
        catch (Exception exception)
        {
            logger.LogError(exception, "Could not get requested measurements");

            return StatusCode(StatusCodes.Status500InternalServerError, exception.Message);
        }
    }

    [MapToApiVersion(2.0)]
    [HttpGet("aggregatedByMonth")]
    public async Task<IActionResult> GetAggregatedByDateAsync([FromQuery] GetAggregatedByDateRequest request)
    {
        try
        {
            var aggregatedByMonth = await measurementsHandler.GetAggregatedByDateAsync(request);
            var result = jsonSerializer.Serialize(aggregatedByMonth);

            return Ok(result);
        }
        catch (MeasurementsNotFoundDuringPeriodException e)
        {
            return NotFound(e.Message);
        }
        catch (Exception exception)
        {
            logger.LogError(exception, "Could not get requested measurements");

            return StatusCode(StatusCodes.Status500InternalServerError, exception.Message);
        }
    }

    [MapToApiVersion(1.0)]
    [HttpGet("aggregatedByYear")]
    public async Task<IActionResult> GetAggregatedByMonthAsync([FromQuery] GetAggregatedByMonthRequest request)
    {
        try
        {
            var aggregatedByYear = await measurementsHandler.GetAggregatedByMonthAsync(request);
            var result = jsonSerializer.Serialize(aggregatedByYear);

            return Ok(result);
        }
        catch (MeasurementsNotFoundDuringPeriodException e)
        {
            return NotFound(e.Message);
        }
        catch (Exception exception)
        {
            logger.LogError(exception, "Could not get requested measurements");

            return StatusCode(StatusCodes.Status500InternalServerError, exception.Message);
        }
    }
}

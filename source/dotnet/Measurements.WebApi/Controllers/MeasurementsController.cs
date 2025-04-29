using Asp.Versioning;
using Energinet.DataHub.Measurements.Application.Exceptions;
using Energinet.DataHub.Measurements.Application.Handlers;
using Energinet.DataHub.Measurements.Application.Requests;
using Energinet.DataHub.Measurements.Infrastructure.Serialization;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace Energinet.DataHub.Measurements.WebApi.Controllers;

[Authorize]
[ApiVersion(2.0)]
[ApiController]
[Route("v{v:apiVersion}/measurements")]
public class MeasurementsController(IMeasurementsHandler measurementsHandler, ILogger<MeasurementsController> logger)
    : ControllerBase
{
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
}

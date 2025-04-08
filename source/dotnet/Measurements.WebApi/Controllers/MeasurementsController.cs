using Energinet.DataHub.Measurements.Application.Exceptions;
using Energinet.DataHub.Measurements.Application.Handlers;
using Energinet.DataHub.Measurements.Application.Requests;
using Energinet.DataHub.Measurements.Infrastructure.Serialization;
using Microsoft.AspNetCore.Mvc;

namespace Energinet.DataHub.Measurements.WebApi.Controllers;

[ApiController]
[Route("measurements")]
public class MeasurementsController(IMeasurementsHandler measurementsHandler, ILogger<MeasurementsController> logger)
    : ControllerBase
{
    [HttpGet]
    // [Authorize] TODO: Uncomment when authentication is implemented in all clients
    [Route("forPeriod")]
    public async Task<IActionResult> GetMeasurementsAsync([FromQuery] GetMeasurementRequest request)
    {
        try
        {
            var measurement = await measurementsHandler.GetMeasurementAsync(request);
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

    [HttpGet]
    // [Authorize] TODO: Uncomment when authentication is implemented in all clients
    [Route("aggregatedByMonth")]
    public async Task<IActionResult> GetAggregatedMeasurementsAsync([FromQuery] GetAggregatedMeasurementsForMonthRequest request)
    {
        try
        {
            var aggregatedMeasurements = await measurementsHandler.GetAggregatedMeasurementsAsync(request);
            var result = new JsonSerializer().Serialize(aggregatedMeasurements);

            return Ok(result);
        }
        catch (MeasurementsNotFoundDuringPeriodException e)
        {
            return NotFound(e.Message);
        }
    }
}

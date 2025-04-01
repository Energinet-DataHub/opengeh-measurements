using Energinet.DataHub.Measurements.Application.Exceptions;
using Energinet.DataHub.Measurements.Application.Handlers;
using Energinet.DataHub.Measurements.Application.Requests;
using Energinet.DataHub.Measurements.Infrastructure.Serialization;
using Microsoft.AspNetCore.Mvc;

namespace Energinet.DataHub.Measurements.WebApi.Controllers;

[ApiController]
[Route("measurements")]
public class MeasurementsController(IMeasurementsHandler measurementsHandler)
    : ControllerBase
{
    [HttpGet]
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
    }

    [HttpGet]
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

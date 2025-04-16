using Asp.Versioning;
using Energinet.DataHub.Measurements.Application.Exceptions;
using Energinet.DataHub.Measurements.Application.Handlers;
using Energinet.DataHub.Measurements.Application.Requests;
using Energinet.DataHub.Measurements.Infrastructure.Serialization;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace Energinet.DataHub.Measurements.WebApi.Controllers;

[ApiVersion(1.0)]
[ApiController]
[Authorize]
public class MeasurementsController(IMeasurementsHandler measurementsHandler)
    : ControllerBase
{
    [MapToApiVersion(1.0)]
    [HttpGet("forPeriod")]
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

    [MapToApiVersion(1.0)]
    [HttpGet("aggregatedByMonth")]
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

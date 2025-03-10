using Energinet.DataHub.Measurements.Application.Exceptions;
using Energinet.DataHub.Measurements.Application.Handlers;
using Energinet.DataHub.Measurements.Application.Requests;
using Microsoft.AspNetCore.Mvc;

namespace Energinet.DataHub.Measurements.WebApi.Controllers;

[ApiController]
[Route("measurements")]
public class MeasurementsController(IMeasurementsHandler measurementsHandler)
    : ControllerBase
{
    [HttpGet]
    public async Task<IActionResult> GetMeasurementAsync([FromQuery] GetMeasurementRequest request)
    {
        try
        {
            var result = await measurementsHandler.GetMeasurementAsync(request);
            return Ok(result);
        }
        catch (MeasurementsNotFoundException e)
        {
            return NotFound(e.Message);
        }
    }
}

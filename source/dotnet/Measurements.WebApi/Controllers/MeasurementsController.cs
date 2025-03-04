using Energinet.DataHub.Measurements.Application.Handlers;
using Microsoft.AspNetCore.Mvc;

namespace Energinet.DataHub.Measurements.WebApi.Controllers;

[ApiController]
[Route("measurements")]
public class MeasurementsController(IMeasurementsHandler measurementsHandler)
    : ControllerBase
{
    [HttpGet]
    public async Task<IActionResult> GetMeasurementAsync(string measurementId)
    {
        var result = await measurementsHandler.GetMeasurementAsync(measurementId, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow);

        return Ok(result);
    }
}

using Energinet.DataHub.Measurements.Application.Handlers;
using Energinet.DataHub.Measurements.WebApi.Requests;
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
        // TODO: Validate request
        var result = await measurementsHandler.GetMeasurementAsync(
            request.MeteringPointId,
            request.StartDate,
            request.EndDate);

        return Ok(result);
    }
}

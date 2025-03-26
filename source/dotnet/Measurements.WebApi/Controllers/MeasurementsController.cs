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
    public async Task<IActionResult> GetMeasurementAsync([FromQuery] GetMeasurementRequest request)
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
    public async Task<IActionResult> GetAggregatedMeasurementsAsync([FromQuery] GetAggregatedMeasurementsForMonthRequest request)
    {
        try
        {
            return await Task.FromResult<IActionResult>(
                BadRequest($"MeteringPointId: {request.MeteringPointId}, year: {request.Year}, month: {request.Month}"));
        }
        catch (MeasurementsNotFoundDuringPeriodException e)
        {
            return NotFound(e.Message);
        }
    }
}

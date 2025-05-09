﻿using Asp.Versioning;
using Energinet.DataHub.Measurements.Application.Exceptions;
using Energinet.DataHub.Measurements.Application.Handlers;
using Energinet.DataHub.Measurements.Application.Requests;
using Energinet.DataHub.Measurements.Infrastructure.Serialization;
using Energinet.DataHub.Measurements.WebApi.Extensions;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace Energinet.DataHub.Measurements.WebApi.Controllers;

[ApiController]
[Authorize]
[ApiVersion(2.0, Deprecated = true)]
[ApiVersion(3.0)]
[Route("v{v:apiVersion}/measurements")]
public class MeasurementsController(
    IMeasurementsHandler measurementsHandler, ILogger<MeasurementsController> logger, IJsonSerializer jsonSerializer)
    : ControllerBase
{
    [MapToApiVersion(2.0)]
    [MapToApiVersion(3.0)]
    [HttpGet("forPeriod")]
    public async Task<IActionResult> GetByPeriodAsync([FromQuery] GetByPeriodRequest request)
    {
        try
        {
            var measurement = await measurementsHandler.GetByPeriodAsync(request);
            var result = jsonSerializer.Serialize(measurement);

            return Ok(result);
        }
        catch (MeasurementsNotFoundException e)
        {
            logger.LogInformation(
                "Measurements not found for metering point id {MeteringPointId} from {StartDate} to {EndDate}",
                request.MeteringPointId.Sanitize(),
                request.StartDate,
                request.EndDate);

            return NotFound(e.Message);
        }
    }

    [MapToApiVersion(2.0)]
    [HttpGet("aggregatedByMonth")]
    public async Task<IActionResult> GetAggregatedByDateAsyncV2([FromQuery] GetAggregatedByDateRequest request)
    {
        try
        {
            var aggregatedByMonth = await measurementsHandler.GetAggregatedByDateAsync(request);
            var result = jsonSerializer.Serialize(aggregatedByMonth);

            return Ok(result);
        }
        catch (MeasurementsNotFoundException e)
        {
            logger.LogInformation(
                "Aggregation by year and month not found for metering point id {MeteringPointId} during {Year}-{Month}",
                request.MeteringPointId.Sanitize(),
                request.Year,
                request.Month);

            return NotFound(e.Message);
        }
    }

    [MapToApiVersion(2.0)]
    [HttpGet("aggregatedByYear")]
    public async Task<IActionResult> GetAggregatedByMonthAsyncV2([FromQuery] GetAggregatedByMonthRequest request)
    {
        try
        {
            var aggregatedByYear = await measurementsHandler.GetAggregatedByMonthAsync(request);
            var result = jsonSerializer.Serialize(aggregatedByYear);

            return Ok(result);
        }
        catch (MeasurementsNotFoundException e)
        {
            logger.LogInformation(
                "Aggregation by year and month not found for metering point id {MeteringPointId} during {Year}",
                request.MeteringPointId.Sanitize(),
                request.Year);

            return NotFound(e.Message);
        }
    }

    [MapToApiVersion(3.0)]
    [HttpGet("aggregatedByDate")]
    public async Task<IActionResult> GetAggregatedByDateAsync([FromQuery] GetAggregatedByDateRequest request)
    {
        try
        {
            var aggregatedByMonth = await measurementsHandler.GetAggregatedByDateAsync(request);
            var result = jsonSerializer.Serialize(aggregatedByMonth);

            return Ok(result);
        }
        catch (MeasurementsNotFoundException e)
        {
            logger.LogInformation(
                "Aggregation by year and month not found for metering point id {MeteringPointId} during {Year}-{Month}",
                request.MeteringPointId.Sanitize(),
                request.Year,
                request.Month);

            return NotFound(e.Message);
        }
    }

    [MapToApiVersion(3.0)]
    [HttpGet("aggregatedByMonth")]
    public async Task<IActionResult> GetAggregatedByMonthAsync([FromQuery] GetAggregatedByMonthRequest request)
    {
        try
        {
            var aggregatedByYear = await measurementsHandler.GetAggregatedByMonthAsync(request);
            var result = jsonSerializer.Serialize(aggregatedByYear);

            return Ok(result);
        }
        catch (MeasurementsNotFoundException e)
        {
            logger.LogInformation(
                "Aggregation by year not found for metering point id {MeteringPointId} during {Year}",
                request.MeteringPointId.Sanitize(),
                request.Year);

            return NotFound(e.Message);
        }
    }

    [MapToApiVersion(3.0)]
    [HttpGet("aggregatedByYear")]
    public async Task<IActionResult> GetAggregatedByYearAsync([FromQuery] GetAggregatedByYearRequest request)
    {
        try
        {
            var aggregatedByYear = await measurementsHandler.GetAggregatedByYearAsync(request);
            var result = jsonSerializer.Serialize(aggregatedByYear);

            return Ok(result);
        }
        catch (MeasurementsNotFoundException e)
        {
            logger.LogInformation(
                "Aggregation by year not found for metering point id {MeteringPointId} for all years",
                request.MeteringPointId.Sanitize());

            return NotFound(e.Message);
        }
    }
}

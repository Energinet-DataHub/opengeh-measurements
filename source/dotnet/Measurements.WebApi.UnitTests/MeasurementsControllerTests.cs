using System.Dynamic;
using System.Net;
using AutoFixture.Xunit2;
using Energinet.DataHub.Core.TestCommon.AutoFixture.Attributes;
using Energinet.DataHub.Measurements.Application.Exceptions;
using Energinet.DataHub.Measurements.Application.Handlers;
using Energinet.DataHub.Measurements.Application.Persistence;
using Energinet.DataHub.Measurements.Application.Requests;
using Energinet.DataHub.Measurements.Application.Responses;
using Energinet.DataHub.Measurements.WebApi.Controllers;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Moq;

namespace Energinet.DataHub.Measurements.WebApi.UnitTests;

public class MeasurementsControllerTests
{
    [Theory]
    [AutoMoqData]
    public async Task GetMeasurementAsync_WhenMeasurementsExists_ReturnValidJson(
        GetMeasurementRequest request,
        Mock<IMeasurementsHandler> measurementsHandler,
        Mock<ILogger<MeasurementsController>> logger)
    {
        // Arrange
        var response = CreateResponse();
        var expected = CreateExpected();
        measurementsHandler
            .Setup(x => x.GetMeasurementAsync(It.IsAny<GetMeasurementRequest>()))
            .ReturnsAsync(response);
        var sut = new MeasurementsController(measurementsHandler.Object, logger.Object);

        // Act
        var actual = (await sut.GetMeasurementAsync(request) as OkObjectResult)!.Value!.ToString();

        // Assert
        Assert.Equal(expected, actual);
    }

    [Theory]
    [AutoMoqData]
    public async Task GetMeasurementAsync_WhenMeasurementsDoNotExist_ReturnsNotFound(
        GetMeasurementRequest request,
        Mock<IMeasurementsHandler> measurementsHandler,
        Mock<ILogger<MeasurementsController>> logger)
    {
        // Arrange
        measurementsHandler
            .Setup(x => x.GetMeasurementAsync(It.IsAny<GetMeasurementRequest>()))
            .ThrowsAsync(new MeasurementsNotFoundDuringPeriodException());
        var sut = new MeasurementsController(measurementsHandler.Object, logger.Object);

        // Act
        var actual = await sut.GetMeasurementAsync(request);

        // Assert
        Assert.IsType<NotFoundObjectResult>(actual);
    }

    [Theory]
    [AutoData]
    public async Task GetMeasurementAsync_WhenMeasurementsUnknownError_ReturnsInternalServerError(
        GetMeasurementRequest request,
        Mock<IMeasurementsHandler> measurementsHandler,
        Mock<ILogger<MeasurementsController>> logger)
    {
        // Arrange
        measurementsHandler
            .Setup(x => x.GetMeasurementAsync(It.IsAny<GetMeasurementRequest>()))
            .ThrowsAsync(new Exception());
        var sut = new MeasurementsController(measurementsHandler.Object, logger.Object);

        // Act
        var actual = await sut.GetMeasurementAsync(request);

        // Assert
        Assert.IsType<ObjectResult>(actual);
        Assert.Equivalent(HttpStatusCode.InternalServerError, ((ObjectResult)actual).StatusCode);
    }

    private static GetMeasurementResponse CreateResponse()
    {
        var measurements = new List<MeasurementResult> { new(CreateMeasurementResult()) };
        var response = GetMeasurementResponse.Create(measurements);
        return response;
    }

    private static string CreateExpected()
    {
        return """{"Points":[{"ObservationTime":"2023-10-15T21:00:00Z","Quantity":42,"Quality":"Measured","Unit":"kWh","Created":"2023-10-15T21:00:00Z"}]}""";
    }

    private static ExpandoObject CreateMeasurementResult()
    {
        var date = new DateTimeOffset(2023, 10, 15, 21, 0, 0, TimeSpan.Zero);

        dynamic raw = new ExpandoObject();
        raw.unit = "kwh";
        raw.observation_time = date;
        raw.quantity = 42;
        raw.quality = "measured";
        raw.created = date;
        return raw;
    }
}

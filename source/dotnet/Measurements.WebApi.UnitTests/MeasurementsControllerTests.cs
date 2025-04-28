using System.Dynamic;
using System.Net;
using AutoFixture.Xunit2;
using Energinet.DataHub.Core.TestCommon.AutoFixture.Attributes;
using Energinet.DataHub.Measurements.Application.Exceptions;
using Energinet.DataHub.Measurements.Application.Handlers;
using Energinet.DataHub.Measurements.Application.Persistence;
using Energinet.DataHub.Measurements.Application.Requests;
using Energinet.DataHub.Measurements.Application.Responses;
using Energinet.DataHub.Measurements.Infrastructure.Serialization;
using Energinet.DataHub.Measurements.WebApi.Controllers;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit.Categories;

namespace Energinet.DataHub.Measurements.WebApi.UnitTests;

[UnitTest]
public class MeasurementsControllerTests
{
    [Theory]
    [AutoMoqData]
    public async Task GetByPeriodAsyncV1_WhenMeasurementsExists_ReturnValidJson(
        GetByPeriodRequest request,
        Mock<IMeasurementsHandler> measurementsHandler,
        Mock<ILogger<MeasurementsController>> logger,
        Mock<IJsonSerializer> jsonSerializer)
    {
        // Arrange
        var response = CreateMeasurementResponse();
        var expected = CreateExpectedMeasurementsByDate();
        measurementsHandler
            .Setup(x => x.GetByPeriodAsyncV1(It.IsAny<GetByPeriodRequest>()))
            .ReturnsAsync(response);
        var sut = new MeasurementsController(measurementsHandler.Object, logger.Object, jsonSerializer.Object);

        // Act
        var actual = (await sut.GetByPeriodAsyncV1(request) as OkObjectResult)!.Value!.ToString();

        // Assert
        Assert.Equal(expected, actual);
    }

    [Theory]
    [AutoMoqData]
    public async Task GetByPeriodAsync_WhenMeasurementsExists_ReturnValidJson(
        GetByPeriodRequest request,
        Mock<IMeasurementsHandler> measurementsHandler,
        Mock<ILogger<MeasurementsController>> logger,
        Mock<IJsonSerializer> jsonSerializer)
    {
        // Arrange
        var response = CreateMeasurementResponse();
        var expected = CreateExpectedMeasurementsByDate();
        measurementsHandler
            .Setup(x => x.GetByPeriodAsync(It.IsAny<GetByPeriodRequest>()))
            .ReturnsAsync(response);
        var sut = new MeasurementsController(measurementsHandler.Object, logger.Object, jsonSerializer.Object);

        // Act
        var actual = (await sut.GetByPeriodAsync(request) as OkObjectResult)!.Value!.ToString();

        // Assert
        Assert.Equal(expected, actual);
    }

    [Theory]
    [AutoMoqData]
    public async Task GetByPeriodAsync_WhenMeasurementsDoNotExist_ReturnsNotFound(
        GetByPeriodRequest request,
        Mock<IMeasurementsHandler> measurementsHandler,
        Mock<ILogger<MeasurementsController>> logger,
        Mock<IJsonSerializer> jsonSerializer)
    {
        // Arrange
        measurementsHandler
            .Setup(x => x.GetByPeriodAsync(It.IsAny<GetByPeriodRequest>()))
            .ThrowsAsync(new MeasurementsNotFoundDuringPeriodException());
        var sut = new MeasurementsController(measurementsHandler.Object, logger.Object, jsonSerializer.Object);

        // Act
        var actual = await sut.GetByPeriodAsync(request);

        // Assert
        Assert.IsType<NotFoundObjectResult>(actual);
    }

    [Theory]
    [AutoData]
    public async Task GetByPeriodAsync_WhenMeasurementsUnknownError_ReturnsInternalServerError(
        GetByPeriodRequest request,
        Mock<IMeasurementsHandler> measurementsHandler,
        Mock<ILogger<MeasurementsController>> logger,
        Mock<IJsonSerializer> jsonSerializer)
    {
        // Arrange
        measurementsHandler
            .Setup(x => x.GetByPeriodAsync(It.IsAny<GetByPeriodRequest>()))
            .ThrowsAsync(new Exception());
        var sut = new MeasurementsController(measurementsHandler.Object, logger.Object, jsonSerializer.Object);

        // Act
        var actual = await sut.GetByPeriodAsync(request);

        // Assert
        Assert.IsType<ObjectResult>(actual);
        Assert.Equivalent(HttpStatusCode.InternalServerError, ((ObjectResult)actual).StatusCode);
    }

    [Theory]
    [AutoData]
    public async Task GetAggregatedByDateAsync_WhenMeasurementsExists_ReturnValidJson(
        GetAggregatedByDateRequest request,
        Mock<IMeasurementsHandler> measurementsHandler,
        Mock<ILogger<MeasurementsController>> logger,
        Mock<IJsonSerializer> jsonSerializer)
    {
        // Arrange
        var response = CreateMeasurementsAggregatedByDateResponse();
        var expected = CreateExpectedMeasurementsAggregatedByDate();
        measurementsHandler
            .Setup(x => x.GetAggregatedByDateAsync(It.IsAny<GetAggregatedByDateRequest>()))
            .ReturnsAsync(response);
        var sut = new MeasurementsController(measurementsHandler.Object, logger.Object, jsonSerializer.Object);

        // Act
        var actual = (await sut.GetAggregatedByDateAsync(request) as OkObjectResult)!.Value!.ToString();

        // Assert
        Assert.Equal(expected, actual);
    }

    [Theory]
    [AutoMoqData]
    public async Task GetAggregatedByDateAsync_WhenMeasurementsDoNotExist_ReturnsNotFound(
        GetAggregatedByDateRequest request,
        Mock<IMeasurementsHandler> measurementsHandler,
        Mock<ILogger<MeasurementsController>> logger,
        Mock<IJsonSerializer> jsonSerializer)
    {
        // Arrange
        measurementsHandler
            .Setup(x => x.GetAggregatedByDateAsync(It.IsAny<GetAggregatedByDateRequest>()))
            .ThrowsAsync(new MeasurementsNotFoundDuringPeriodException());
        var sut = new MeasurementsController(measurementsHandler.Object, logger.Object, jsonSerializer.Object);

        // Act
        var actual = await sut.GetAggregatedByDateAsync(request);

        // Assert
        Assert.IsType<NotFoundObjectResult>(actual);
    }

    [Theory]
    [AutoData]
    public async Task GetAggregatedByDateAsync_WhenMeasurementsUnknownError_ReturnsInternalServerError(
        GetAggregatedByDateRequest request,
        Mock<IMeasurementsHandler> measurementsHandler,
        Mock<ILogger<MeasurementsController>> logger,
        Mock<IJsonSerializer> jsonSerializer)
    {
        // Arrange
        measurementsHandler
            .Setup(x => x.GetAggregatedByDateAsync(It.IsAny<GetAggregatedByDateRequest>()))
            .ThrowsAsync(new Exception());
        var sut = new MeasurementsController(measurementsHandler.Object, logger.Object, jsonSerializer.Object);

        // Act
        var actual = await sut.GetAggregatedByDateAsync(request);

        // Assert
        Assert.IsType<ObjectResult>(actual);
        Assert.Equivalent(HttpStatusCode.InternalServerError, ((ObjectResult)actual).StatusCode);
    }

    [Theory]
    [AutoData]
    public async Task GetAggregatedByMonthAsync_WhenMeasurementsExists_ReturnValidJson(
        GetAggregatedByMonthRequest request,
        Mock<IMeasurementsHandler> measurementsHandler,
        Mock<ILogger<MeasurementsController>> logger,
        Mock<IJsonSerializer> jsonSerializer)
    {
        // Arrange
        var response = CreateMeasurementsAggregatedByMonthResponse();
        var expected = CreateExpectedMeasurementsAggregatedByMonth();
        measurementsHandler
            .Setup(x => x.GetAggregatedByMonthAsync(It.IsAny<GetAggregatedByMonthRequest>()))
            .ReturnsAsync(response);
        var sut = new MeasurementsController(measurementsHandler.Object, logger.Object, jsonSerializer.Object);

        // Act
        var actual = (await sut.GetAggregatedByMonthAsync(request) as OkObjectResult)!.Value!.ToString();

        // Assert
        Assert.Equal(expected, actual);
    }

    [Theory]
    [AutoMoqData]
    public async Task GetAggregatedByMonthAsync_WhenMeasurementsDoNotExist_ReturnsNotFound(
        GetAggregatedByMonthRequest request,
        Mock<IMeasurementsHandler> measurementsHandler,
        Mock<ILogger<MeasurementsController>> logger,
        Mock<IJsonSerializer> jsonSerializer)
    {
        // Arrange
        measurementsHandler
            .Setup(x => x.GetAggregatedByMonthAsync(It.IsAny<GetAggregatedByMonthRequest>()))
            .ThrowsAsync(new MeasurementsNotFoundDuringPeriodException());
        var sut = new MeasurementsController(measurementsHandler.Object, logger.Object, jsonSerializer.Object);

        // Act
        var actual = await sut.GetAggregatedByMonthAsync(request);

        // Assert
        Assert.IsType<NotFoundObjectResult>(actual);
    }

    [Theory]
    [AutoData]
    public async Task GetAggregatedByMonthAsync_WhenMeasurementsUnknownError_ReturnsInternalServerError(
        GetAggregatedByMonthRequest request,
        Mock<IMeasurementsHandler> measurementsHandler,
        Mock<ILogger<MeasurementsController>> logger,
        Mock<IJsonSerializer> jsonSerializer)
    {
        // Arrange
        measurementsHandler
            .Setup(x => x.GetAggregatedByMonthAsync(It.IsAny<GetAggregatedByMonthRequest>()))
            .ThrowsAsync(new Exception());
        var sut = new MeasurementsController(measurementsHandler.Object, logger.Object, jsonSerializer.Object);

        // Act
        var actual = await sut.GetAggregatedByMonthAsync(request);

        // Assert
        Assert.IsType<ObjectResult>(actual);
        Assert.Equivalent(HttpStatusCode.InternalServerError, ((ObjectResult)actual).StatusCode);
    }

    private static GetMeasurementResponse CreateMeasurementResponse()
    {
        var measurements = new List<MeasurementResult> { new(CreateMeasurementResult()) };
        var response = GetMeasurementResponse.Create(measurements);
        return response;
    }

    private static GetMeasurementsAggregatedByDateResponse CreateMeasurementsAggregatedByDateResponse()
    {
        var measurements = new List<AggregatedMeasurementsResult> { new(CreateAggregatedMeasurementResult()) };
        var response = GetMeasurementsAggregatedByDateResponse.Create(measurements);
        return response;
    }

    private static GetMeasurementsAggregatedByMonthResponse CreateMeasurementsAggregatedByMonthResponse()
    {
        var measurements = new List<AggregatedMeasurementsResult> { new(CreateAggregatedMeasurementResult()) };
        var response = GetMeasurementsAggregatedByMonthResponse.Create(measurements);
        return response;
    }

    private static string CreateExpectedMeasurementsByDate()
    {
        return """{"Points":[{"ObservationTime":"2023-10-15T21:00:00Z","Quantity":42,"Quality":"Measured","Unit":"kWh","Resolution":"Hourly","Created":"2023-10-15T21:00:00Z","TransactionCreated":"2023-10-15T21:00:00Z"}]}""";
    }

    private static string CreateExpectedMeasurementsAggregatedByDate()
    {
        return """{"MeasurementAggregations":[{"Date":"2023-09-02","Quantity":42,"Quality":"Measured","Unit":"kWh","MissingValues":true,"ContainsUpdatedValues":true}]}""";
    }

    private static string CreateExpectedMeasurementsAggregatedByMonth()
    {
        return """{"MeasurementAggregations":[{"YearMonth":"2023-09","Quantity":42,"Quality":"Measured","Unit":"kWh"}]}""";
    }

    private static ExpandoObject CreateMeasurementResult()
    {
        var date = new DateTimeOffset(2023, 10, 15, 21, 0, 0, TimeSpan.Zero);

        dynamic raw = new ExpandoObject();
        raw.unit = "kwh";
        raw.observation_time = date;
        raw.quantity = 42;
        raw.quality = "measured";
        raw.resolution = "PT1H";
        raw.created = date;
        raw.transaction_creation_datetime = date;
        return raw;
    }

    private static ExpandoObject CreateAggregatedMeasurementResult()
    {
        var minDate = new DateTimeOffset(2023, 09, 01, 22, 0, 0, TimeSpan.Zero);
        var maxDate = new DateTimeOffset(2023, 09, 30, 21, 0, 0, TimeSpan.Zero);

        dynamic raw = new ExpandoObject();
        raw.min_observation_time = minDate;
        raw.max_observation_time = maxDate;
        raw.aggregated_quantity = 42;
        raw.qualities = new[] { "measured" };
        raw.resolutions = new[] { "PT1H" };
        raw.units = new[] { "kWh" };
        raw.point_count = 1;
        raw.observation_updates = 2;
        return raw;
    }
}

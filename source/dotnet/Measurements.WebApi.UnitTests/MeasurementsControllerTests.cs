using System.Dynamic;
using Energinet.DataHub.Measurements.Application.Handlers;
using Energinet.DataHub.Measurements.Application.Persistence;
using Energinet.DataHub.Measurements.Application.Requests;
using Energinet.DataHub.Measurements.Application.Responses;
using Energinet.DataHub.Measurements.Domain;
using Energinet.DataHub.Measurements.WebApi.Controllers;
using Microsoft.AspNetCore.Mvc;
using Moq;
using NodaTime;

namespace Energinet.DataHub.Measurements.WebApi.UnitTests;

public class MeasurementsControllerTests
{
    [Fact]
    public void GetMeasurementAsync_WhenMeasurenentsExists_ReturnValidJson()
    {
        // Arrange
        var date = new DateTimeOffset(2023, 10, 15, 21, 0, 0, TimeSpan.Zero);
        var instant = Instant.FromUtc(2023, 10, 15, 21, 00, 00);
        var measurementsHandler = new Mock<IMeasurementsHandler>();
        var controller = new MeasurementsController(measurementsHandler.Object);
        var request = new GetMeasurementRequest("1234567890", date, date.AddDays(1));
        var measurements = new List<MeasurementResult>
        {
            new(CreateMeasurementResult(instant.ToDateTimeOffset())),
            new(CreateMeasurementResult(instant.ToDateTimeOffset())),
            new(CreateMeasurementResult(instant.ToDateTimeOffset())),
        };
        var sut = new MeasurementsController(measurementsHandler.Object);

        // Act
        var actual = sut.GetMeasurementAsync(request);

        // Assert
        Assert.NotNull(actual);
    }

    private static ExpandoObject CreateMeasurementResult(DateTimeOffset date, string unit = "kwh", string quality = "measured")
    {
        dynamic raw = new ExpandoObject();
        raw.unit = unit;
        raw.observation_time = date;
        raw.quantity = 42;
        raw.quality = quality;
        raw.created = date;
        return raw;
    }
}

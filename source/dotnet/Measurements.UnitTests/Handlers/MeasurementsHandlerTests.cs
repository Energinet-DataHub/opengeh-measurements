﻿using System.Dynamic;
using AutoFixture.Xunit2;
using Energinet.DataHub.Measurements.Application.Persistence;
using Energinet.DataHub.Measurements.Application.Requests;
using Energinet.DataHub.Measurements.Domain;
using Energinet.DataHub.Measurements.Infrastructure.Handlers;
using Moq;
using NodaTime;
using Xunit;

namespace Energinet.DataHub.Measurements.UnitTests.Handlers;

public class MeasurementsHandlerTests
{
    [Theory]
    [InlineAutoData]
    public async Task GetMeasurementAsync_WhenCalled_ThenReturnsMeasurementsForPeriod(
        Mock<IMeasurementRepository> measurementRepositoryMock)
    {
        // Arrange
        var now = new DateTimeOffset(2021, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var request = new GetMeasurementRequest("123456789", now, now.AddDays(1));
        dynamic raw = new ExpandoObject();
        raw.metering_point_id = "123456789";
        raw.unit = "KWH";
        raw.observation_time = now;
        raw.quantity = 42;
        raw.quality = "measured";
        var measurementResult = new MeasurementResult(raw);
        measurementRepositoryMock
            .Setup(x => x.GetMeasurementAsync(It.IsAny<string>(), It.IsAny<Instant>(), It.IsAny<Instant>()))
            .Returns(AsyncEnumerable.Repeat(measurementResult, 1));
        var sut = new MeasurementsHandler(measurementRepositoryMock.Object);

        // Act
        var actual = await sut.GetMeasurementAsync(request);
        var actualPoint = actual.Points.First();

        // Assert
        Assert.Equal(request.MeteringPointId, actual.MeteringPointId);
        Assert.Equal(42, actualPoint.Quantity);
        Assert.Equal(Quality.Measured, actualPoint.Quality);
    }
}

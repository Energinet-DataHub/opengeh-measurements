﻿using System.Dynamic;
using AutoFixture.Xunit2;
using Energinet.DataHub.Measurements.Application.Exceptions;
using Energinet.DataHub.Measurements.Application.Persistence;
using Energinet.DataHub.Measurements.Application.Requests;
using Energinet.DataHub.Measurements.Domain;
using Energinet.DataHub.Measurements.Infrastructure.Handlers;
using Moq;
using NodaTime;
using Xunit;
using Xunit.Categories;

namespace Energinet.DataHub.Measurements.UnitTests.Infrastructure.Handlers;

[UnitTest]
public class MeasurementsHandlerTests
{
    [Theory]
    [InlineAutoData]
    public async Task GetMeasurementAsync_WhenMeasurementsExist_ThenReturnsMeasurementsForPeriod(
        Mock<IMeasurementsRepository> measurementRepositoryMock)
    {
        // Arrange
        var date = new DateTimeOffset(2021, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var request = new GetMeasurementRequest("123456789", date, date.AddDays(1));
        var raw = CreateRaw(date);
        var measurementResult = new MeasurementsResult(raw);
        measurementRepositoryMock
            .Setup(x => x.GetMeasurementsAsync(It.IsAny<string>(), It.IsAny<Instant>(), It.IsAny<Instant>()))
            .Returns(AsyncEnumerable.Repeat(measurementResult, 1));
        var sut = new MeasurementsHandler(measurementRepositoryMock.Object);

        // Act
        var actual = await sut.GetMeasurementAsync(request);
        var actualPoint = actual.Points.Single();

        // Assert
        Assert.Equal(date, actualPoint.ObservationTime.ToDateTimeOffset());
        Assert.Equal(42, actualPoint.Quantity);
        Assert.Equal(Quality.Measured, actualPoint.Quality);
        Assert.Equal(Unit.kWh, actualPoint.Unit);
        Assert.Equal(date, actualPoint.Created.ToDateTimeOffset());
    }

    [Fact]
    public async Task GetMeasurementsAsync_WhenMeasurementsNotExist_ThenThrowsNotFoundException()
    {
        // Arrange
        var date = new DateTimeOffset(2021, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var request = new GetMeasurementRequest("123456789", date, date.AddDays(1));
        var measurementRepositoryMock = new Mock<IMeasurementsRepository>();
        measurementRepositoryMock
            .Setup(x => x.GetMeasurementsAsync(It.IsAny<string>(), It.IsAny<Instant>(), It.IsAny<Instant>()))
            .Returns(AsyncEnumerable.Empty<MeasurementsResult>());
        var sut = new MeasurementsHandler(measurementRepositoryMock.Object);

        // Act
        // Assert
        await Assert.ThrowsAsync<MeasurementsNotFoundDuringPeriodException>(() => sut.GetMeasurementAsync(request));
    }

    private static dynamic CreateRaw(DateTimeOffset now)
    {
        dynamic raw = new ExpandoObject();
        raw.unit = "kwh";
        raw.observation_time = now;
        raw.quantity = 42;
        raw.quality = "measured";
        raw.created = now;
        return raw;
    }
}

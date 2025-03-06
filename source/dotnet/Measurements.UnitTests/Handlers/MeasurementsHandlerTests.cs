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
        var now = SystemClock.Instance.GetCurrentInstant();
        measurementRepositoryMock
            .Setup(x => x.GetMeasurementAsync(It.IsAny<string>(), It.IsAny<Instant>(), It.IsAny<Instant>()))
            .ReturnsAsync(new Measurement("123456789", Unit.KWh, [new Point(now, 42, Quality.Measured)]));
        var sut = new MeasurementsHandler(measurementRepositoryMock.Object);

        var request = new GetMeasurementRequest("123456789", now, now.Plus(Duration.FromDays(1)));

        // Act
        var actual = await sut.GetMeasurementAsync(request);
        var actualPoint = actual.Points.First();

        // Assert
        Assert.Equal(request.MeteringPointId, actual.MeteringPointId);
        Assert.Equal(42, actualPoint.Quantity);
        Assert.Equal(Quality.Measured.ToString(), actualPoint.Quality);
    }
}

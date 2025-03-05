using Energinet.DataHub.Measurements.Infrastructure.Handlers;
using NodaTime;

namespace Energinet.DataHub.Measurements.UnitTests.Handlers;

public class MeasurementsHandlerTests
{
    [Fact]
    public async Task GetMeasurementAsync_WhenCalled_ThenReturnsMeasurementsForPeriod()
    {
        // Arrange
        var sut = new MeasurementsHandler();
        var meteringPointId = Guid.NewGuid().ToString();
        var startDate = SystemClock.Instance.GetCurrentInstant();
        var endDate = SystemClock.Instance.GetCurrentInstant();

        // Act
        var actual = await sut.GetMeasurementAsync(meteringPointId, startDate, endDate);
        var actualPoint = actual.Points.First();

        // Assert
        Assert.Equal(meteringPointId, actual.MeteringPointId);
        Assert.Equal(42, actualPoint.Quantity);
    }
}

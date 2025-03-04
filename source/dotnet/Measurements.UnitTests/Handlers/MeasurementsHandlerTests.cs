using AutoFixture.Xunit2;
using Energinet.DataHub.Measurements.Infrastructure.Handlers;

namespace Energinet.DataHub.Measurements.UnitTests.Handlers;

public class MeasurementsHandlerTests
{
    [Fact]
    public async Task GetMeasurementAsync_WhenCalled_ThenReturnsMeasurementsForPeriod()
    {
        // Arrange
        var sut = new MeasurementsHandler();
        var meteringPointId = Guid.NewGuid().ToString();
        var startDate = new DateTimeOffset(2024, 6, 1, 0, 0, 0, TimeSpan.Zero);
        var endDate = new DateTimeOffset(2024, 6, 2, 0, 0, 0, TimeSpan.Zero);

        // Act
        var actual = await sut.GetMeasurementAsync(meteringPointId, startDate, endDate);
        var actualTransaction = actual.Transactions.First();

        // Assert
        Assert.Equal(startDate, actualTransaction.StartTimestamp);
        Assert.Equal(endDate, actualTransaction.EndTimestamp);
        Assert.Equal(42, actualTransaction.Quantity);
    }
}

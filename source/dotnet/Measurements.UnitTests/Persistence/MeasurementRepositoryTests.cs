using Energinet.DataHub.Measurements.Domain;
using Energinet.DataHub.Measurements.Infrastructure.Persistence;
using NodaTime;
using Xunit;

namespace Energinet.DataHub.Measurements.UnitTests.Persistence;

public class MeasurementRepositoryTests
{
    [Fact]
    public async Task GetMeasurementAsync_WhenCalled_ReturnsMeasurement()
    {
        // Arrange
        const string meteringPointId = "1234567890";
        var from = Instant.FromUtc(2021, 1, 1, 0, 0);
        var to = Instant.FromUtc(2021, 1, 2, 0, 0);
        var expected = new Measurement(meteringPointId, Unit.KWh, []);
        var sut = new MeasurementRepository();

        // Act
        var actual = await sut.GetMeasurementAsync(meteringPointId, from, to);

        // Assert
        Assert.Equal(expected, actual);
    }
}

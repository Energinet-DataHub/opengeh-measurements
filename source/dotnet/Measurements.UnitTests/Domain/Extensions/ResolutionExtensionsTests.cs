using Energinet.DataHub.Measurements.Domain;
using Energinet.DataHub.Measurements.Domain.Extensions;
using NodaTime;
using Xunit;

namespace Energinet.DataHub.Measurements.UnitTests.Domain.Extensions;

public class ResolutionExtensionsTests
{
    [Theory]
    [InlineData(Resolution.QuarterHourly, 96)]
    [InlineData(Resolution.Hourly, 24)]
    [InlineData(Resolution.Daily, 1)]
    [InlineData(Resolution.Monthly, 1)]
    [InlineData(Resolution.Yearly, 1)]
    public void GetExpectedPointCount_ReturnsExpectedCount(Resolution resolution, int expectedPointCount)
    {
        // Arrange
        const int hours = 23;
        var minObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow);
        var maxObservationTime = minObservationTime.Plus(Duration.FromHours(hours));

        // Act
        var actual = resolution.GetExpectedPointCount(maxObservationTime, minObservationTime);

        // Assert
        Assert.Equal(expectedPointCount, actual);
    }
}

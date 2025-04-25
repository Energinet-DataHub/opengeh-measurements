using Energinet.DataHub.Measurements.Application.Responses;
using Energinet.DataHub.Measurements.Domain;
using Xunit;

namespace Energinet.DataHub.Measurements.UnitTests.Application.Responses;

public class PointCountCalculatorTests
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
        const int hours = 24;

        // Act
        var actual = PointCountCalculator.GetExpectedPointCount(resolution, hours);

        // Assert
        Assert.Equal(expectedPointCount, actual);
    }
}

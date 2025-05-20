using Energinet.DataHub.Measurements.Application.Extensions;
using Energinet.DataHub.Measurements.Domain;
using NodaTime.Text;
using Xunit;

namespace Energinet.DataHub.Measurements.UnitTests.Domain.Extensions;

public class ResolutionExtensionsTests
{
    [Theory]
    [InlineData("2024-03-29T22:45:00Z", 96)]
    [InlineData("2024-03-29T23:00:00Z", 96)]
    [InlineData("2024-03-29T23:15:00Z", 96)]
    [InlineData("2024-03-30T23:00:00Z", 92)]
    [InlineData("2024-03-31T22:00:00Z", 96)]
    [InlineData("2024-10-25T22:00:00Z", 96)]
    [InlineData("2024-10-26T22:00:00Z", 100)]
    [InlineData("2024-10-27T23:00:00Z", 96)]
    public void GetExpectedPointCount_WhenQuarterHourlyResolution_ReturnsExpectedCount(
        string timestamp, int expectedPointCount)
    {
        // Arrange
        var observationTime = InstantPattern.ExtendedIso.Parse(timestamp).Value;

        // Act
        var actual = Resolution.QuarterHourly.GetExpectedPointsForPeriod(observationTime, 1);

        // Assert
        Assert.Equal(expectedPointCount, actual);
    }

    [Theory]
    [InlineData("2024-03-29T23:00:00Z", 1, 24)]
    [InlineData("2024-03-30T23:00:00Z", 1, 23)]
    [InlineData("2024-03-31T22:00:00Z", 1, 24)]
    [InlineData("2024-10-25T22:00:00Z", 1, 24)]
    [InlineData("2024-10-26T22:00:00Z", 1, 25)]
    [InlineData("2024-10-27T23:00:00Z", 1, 24)]
    [InlineData("2024-10-23T22:00:00Z", 2, 48)]
    [InlineData("2024-10-26T12:34:56Z", 4, 97)]
    public void GetExpectedPointCount_WhenHourlyResolution_ReturnsExpectedCount(
        string timestamp, int numberOfDays, int expectedPointCount)
    {
        // Arrange
        var observationTime = InstantPattern.ExtendedIso.Parse(timestamp).Value;

        // Act
        var actual = Resolution.Hourly.GetExpectedPointsForPeriod(observationTime, numberOfDays);

        // Assert
        Assert.Equal(expectedPointCount, actual);
    }

    [Theory]
    [InlineData("2024-03-29T23:00:00Z", 1)]
    [InlineData("2024-03-30T23:00:00Z", 1)]
    [InlineData("2024-03-31T22:00:00Z", 1)]
    [InlineData("2024-10-25T22:00:00Z", 1)]
    [InlineData("2024-10-26T22:00:00Z", 1)]
    [InlineData("2024-10-27T23:00:00Z", 1)]
    public void GetExpectedPointCount_WhenDailyResolution_ReturnsExpectedCount(
        string timestamp, int expectedPointCount)
    {
        // Arrange
        var observationTime = InstantPattern.ExtendedIso.Parse(timestamp).Value;

        // Act
        var actual = Resolution.Daily.GetExpectedPointsForPeriod(observationTime, 1);

        // Assert
        Assert.Equal(expectedPointCount, actual);
    }
}

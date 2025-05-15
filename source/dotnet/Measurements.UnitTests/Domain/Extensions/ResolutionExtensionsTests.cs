using Energinet.DataHub.Measurements.Application.Extensions;
using Energinet.DataHub.Measurements.Domain;
using NodaTime.Text;
using Xunit;

namespace Energinet.DataHub.Measurements.UnitTests.Domain.Extensions;

public class ResolutionExtensionsTests
{
    [Theory]
    [InlineData("2024-03-29T23:00:00Z", 96)]
    [InlineData("2024-03-30T23:00:00Z", 92)]
    [InlineData("2024-03-31T22:00:00Z", 96)]
    [InlineData("2024-10-25T23:00:00Z", 96)]
    [InlineData("2024-10-26T22:00:00Z", 100)]
    [InlineData("2024-10-27T23:00:00Z", 96)]
    public void GetExpectedPointCount_WhenQuarterHourlyResolution_ReturnsExpectedCount(
        string timestamp, int expectedPointCount)
    {
        // Arrange
        var fromInstant = InstantPattern.ExtendedIso.Parse(timestamp).Value;

        // Act
        var actual = Resolution.QuarterHourly.GetExpectedPointCount(fromInstant);

        // Assert
        Assert.Equal(expectedPointCount, actual);
    }

    [Theory]
    [InlineData("2024-03-29T23:00:00Z", 24)]
    [InlineData("2024-03-30T23:00:00Z", 23)]
    [InlineData("2024-03-31T22:00:00Z", 24)]
    [InlineData("2024-10-25T23:00:00Z", 24)]
    [InlineData("2024-10-26T22:00:00Z", 25)]
    [InlineData("2024-10-27T23:00:00Z", 24)]
    public void GetExpectedPointCount_WhenHourlyResolution_ReturnsExpectedCount(
        string timestamp, int expectedPointCount)
    {
        // Arrange
        var observationTime = InstantPattern.ExtendedIso.Parse(timestamp).Value;

        // Act
        var actual = Resolution.Hourly.GetExpectedPointCount(observationTime);

        // Assert
        Assert.Equal(expectedPointCount, actual);
    }

    [Theory]
    [InlineData("2024-03-29T23:00:00Z", 1)]
    [InlineData("2024-03-30T22:00:00Z", 1)]
    [InlineData("2024-10-26T22:00:00Z", 1)]
    [InlineData("2024-10-27T23:00:00Z", 1)]
    public void GetExpectedPointCount_WhenDailyResolution_ReturnsExpectedCount(
        string timestamp, int expectedPointCount)
    {
        // Arrange
        var observationTime = InstantPattern.ExtendedIso.Parse(timestamp).Value;

        // Act
        var actual = Resolution.Daily.GetExpectedPointCount(observationTime);

        // Assert
        Assert.Equal(expectedPointCount, actual);
    }
}

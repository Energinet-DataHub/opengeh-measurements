using System.Globalization;
using Energinet.DataHub.Measurements.Infrastructure.Extensions;
using NodaTime;
using Xunit;
using Xunit.Categories;

namespace Energinet.DataHub.Measurements.UnitTests.Infrastructure.Extensions;

[UnitTest]
public class DateAndTimeExtensionsTests
{
    [Theory]
    [InlineData(2025, 3, 30, "2025-03-29T23:00:00Z")]
    [InlineData(2025, 3, 31, "2025-03-30T22:00:00Z")]
    [InlineData(2025, 4, 1, "2025-03-31T22:00:00Z")]
    [InlineData(2025, 10, 25, "2025-10-24T22:00:00Z")]
    [InlineData(2025, 10, 26, "2025-10-25T22:00:00Z")]
    [InlineData(2025, 10, 27, "2025-10-26T23:00:00Z")]
    public void ToUtcString_WhenCalledWithLocalDate_ReturnsUtcString(
        int year, int month, int day, string expected)
    {
        // Arrange
        var date = new LocalDate(year, month, day);

        // Act
        var actual = date.ToUtcString();

        // Assert
        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData(2025, 3, 30, "2025-03-29T23:00:00Z")]
    [InlineData(2025, 3, 31, "2025-03-30T22:00:00Z")]
    [InlineData(2025, 4, 1, "2025-03-31T22:00:00Z")]
    [InlineData(2025, 10, 25, "2025-10-24T22:00:00Z")]
    [InlineData(2025, 10, 26, "2025-10-25T22:00:00Z")]
    [InlineData(2025, 10, 27, "2025-10-26T23:00:00Z")]
    public void ToDateTimeOffSetAtMidnight_WhenCalledWithLocalDate_ReturnsDateTimeOffSet(int year, int month, int day, string expected)
    {
        // Arrange
        var date = new LocalDate(year, month, day);

        // Act
        var actual = date.ToDateTimeOffSetAtMidnight();

        // Assert
        var actualString = actual.UtcDateTime.ToString("yyyy-MM-ddTHH:mm:ss'Z'", CultureInfo.InvariantCulture);
        Assert.Equal(expected, actualString);
    }

    [Theory]
    [InlineData(2025, 3, 30, "2025-03-29T23:00:00Z")]
    [InlineData(2025, 3, 31, "2025-03-30T22:00:00Z")]
    [InlineData(2025, 4, 1, "2025-03-31T22:00:00Z")]
    [InlineData(2025, 10, 25, "2025-10-24T22:00:00Z")]
    [InlineData(2025, 10, 26, "2025-10-25T22:00:00Z")]
    [InlineData(2025, 10, 27, "2025-10-26T23:00:00Z")]
    public void ToInstantAtMidnight_WhenCalledWithLocalDate_ReturnsInstant(int year, int month, int day, string expected)
    {
        // Arrange
        var date = new LocalDate(year, month, day);

        // Act
        var actual = date.ToInstantAtMidnight();

        // Assert
        var actualString = actual.ToString("yyyy-MM-ddTHH:mm:ss'Z'", CultureInfo.InvariantCulture);
        Assert.Equal(expected, actualString);
    }

    [Theory]
    [InlineData(2024, 10, "2024-09-30T22:00:00Z", "2024-10-31T23:00:00Z")]
    [InlineData(2024, 12, "2024-11-30T23:00:00Z", "2024-12-31T23:00:00Z")]
    [InlineData(2025, 3, "2025-02-28T23:00:00Z", "2025-03-31T22:00:00Z")]
    public void ToDateIntervalIncludingLastDay_WhenCalledWithLocalDate_ReturnsDateInterval(
        int year, int month, string expectedStart, string expectedEnd)
    {
        // Arrange
        var yearMonth = new YearMonth(year, month);

        // Act
        var (actualStart, actualEnd) = yearMonth.ToDateIntervalIncludingLastDay();

        // Assert
        Assert.Equal(expectedStart, actualStart.ToUtcString());
        Assert.Equal(expectedEnd, actualEnd.ToUtcString());
    }

    [Theory]
    [InlineData(2025, 3, 30, "2025-03-29T23:00:00Z")]
    [InlineData(2025, 3, 31, "2025-03-30T22:00:00Z")]
    [InlineData(2025, 4, 1, "2025-03-31T22:00:00Z")]
    [InlineData(2025, 10, 25, "2025-10-24T22:00:00Z")]
    [InlineData(2025, 10, 26, "2025-10-25T22:00:00Z")]
    [InlineData(2025, 10, 27, "2025-10-26T23:00:00Z")]
    public void ToUtcString_WhenCalledWithInstant_ReturnsUtcString(int year, int month, int day, string expected)
    {
        // Arrange
        var date = Instant.FromUtc(year, month, day, 0, 0);

        // Act
        var actual = date.ToUtcString();

        // Assert
        Assert.Equal(expected, actual);
    }
}

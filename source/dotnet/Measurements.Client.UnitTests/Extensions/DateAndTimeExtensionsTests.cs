using System.Globalization;
using Energinet.DataHub.Measurements.Client.Extensions;
using NodaTime;
using Xunit.Categories;

namespace Energinet.DataHub.Measurements.Client.UnitTests.Extensions;

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
    public void ToUtcString_WhenCalled_ReturnsUtcString(
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
    public void ToDateTimeOffset_WhenCalled_ReturnsDateTimeOffset(
        int year, int month, int day, string expected)
    {
        // Arrange
        var date = new LocalDate(year, month, day);

        // Act
        var actual = date.ToUtcDateTimeOffset();

        // Assert
        Assert.Equal(expected, actual.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture));
    }

    [Theory]
    [InlineData(2025, 3, 29, 23, -1,  "2025-03-30")]
    [InlineData(2025, 3, 30, 22, -2, "2025-03-31")]
    [InlineData(2025, 3, 31, 22, -2, "2025-04-01")]
    [InlineData(2025, 10, 24, 22, -2, "2025-10-25")]
    [InlineData(2025, 10, 25, 22, -2, "2025-10-26")]
    [InlineData(2025, 10, 26, 23, -1, "2025-10-27")]
    public void ToLocalDate_WhenCalled_ReturnsLocalDate(int year, int month, int day, int hour, int offset, string expected)
    {
        // Arrange
        var date = new DateTimeOffset(year, month, day, hour, 0, 0, TimeSpan.FromHours(offset));

        // Act
        var actual = date.ToLocalDate();

        // Assert
        Assert.Equal(expected, actual.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture));
    }

    [Fact]
    public void ToFormattedString_WhenCalled_ReturnsFormattedString()
    {
        // Arrange
        var date = new DateTimeOffset(2025, 3, 30, 0, 0, 0, TimeSpan.Zero);

        // Act
        var actual = date.ToFormattedString();

        // Assert
        Assert.Equal("2025-03-30T00:00:00Z", actual);
    }
}

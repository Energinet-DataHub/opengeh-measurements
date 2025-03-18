using System.Globalization;
using Energinet.DataHub.Measurements.Client.Extensions;
using NodaTime;
using Xunit.Categories;

namespace Energinet.DataHub.Measurements.Client.Tests.Extensions;

[UnitTest]
public class LocalDateExtensionsTests
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
        var result = date.ToUtcString();

        // Assert
        Assert.Equal(expected, result);
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
        var result = date.ToUtcDateTimeOffset();

        // Assert
        Assert.Equal(expected, result.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture));
    }
}

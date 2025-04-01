using Energinet.DataHub.Measurements.Infrastructure.Extensions;
using NodaTime;
using Xunit;
using Xunit.Categories;

namespace Energinet.DataHub.Measurements.UnitTests.Extensions;

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
}

using Energinet.DataHub.Measurements.Domain;
using Energinet.DataHub.Measurements.Infrastructure.Extensions;
using NodaTime;
using Xunit;
using Xunit.Categories;

namespace Energinet.DataHub.Measurements.UnitTests.Domain;

[UnitTest]
public class YearTests
{
    [Fact]
    public void GetYear_ReturnsExpectedValue()
    {
        // Arrange
        const int expected = 2025;

        // Act
        var actual = new Year(expected);

        // Assert
        Assert.Equal(expected, actual.GetYear());
    }

    [Theory]
    [InlineData(10000)]
    [InlineData(-9999)]
    public void Contructor_WhenInvalidYear_ThenThrowsValidationException(int year)
    {
        // Act & Assert
        var exception = Assert.Throws<ArgumentOutOfRangeException>(() => new Year(year));
        Assert.Equal("Value should be in range [-9998-9999] (Parameter 'year')", exception.Message[..56]);
    }

    [Fact]
    public void ToDateInterval_WhenValidDates_ThenReturnsCorrectInterval()
    {
        // Arrange
        var year = new Year(2025);
        var expectedStartDate = new LocalDate(2025, 1, 1).ToUtcString();
        var expectedEndDate = new LocalDate(2026, 1, 1).ToUtcString();

        // Act
        var (actualStartDate, actualEndDate) = year.ToDateIntervalIncludingLastDay();

        // Assert
        Assert.Equal(expectedStartDate, actualStartDate.ToUtcString());
        Assert.Equal(expectedEndDate, actualEndDate.ToUtcString());
    }
}

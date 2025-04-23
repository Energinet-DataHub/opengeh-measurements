using Energinet.DataHub.Measurements.Domain;
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
    public void Year_Constructor_InvalidYear_ThrowsValidationException(int year)
    {
        // Act & Assert
        var exception = Assert.Throws<ArgumentOutOfRangeException>(() => new Year(year));
        Assert.Equal("Value should be in range [-9998-9999] (Parameter 'year')", exception.Message[..56]);
    }

    [Fact]
    public void Year_ToDateInterval_ReturnsCorrectInterval()
    {
        // Arrange
        var year = new Year(2025);
        var expectedStartDate = new LocalDate(2025, 1, 1);
        var expectedEndDate = new LocalDate(2025, 12, 31);

        // Act
        var (startDate, endDate) = year.ToDateInterval();

        // Assert
        Assert.Equal(expectedStartDate, startDate);
        Assert.Equal(expectedEndDate, endDate);
    }

    [Theory]
    [InlineData(2025, 2025, true)]
    [InlineData(2025, 2024, false)]
    public void Year_Equals_ReturnsExpectedResult(int testYear1, int testYear2, bool expectedResult)
    {
        // Arrange
        var year1 = new Year(testYear1);
        var year2 = new Year(testYear2);

        // Act
        var actual = year1.Equals(year2);

        // Assert
        Assert.Equal(expectedResult, actual);
    }
}

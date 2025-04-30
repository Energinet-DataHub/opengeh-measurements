using Energinet.DataHub.Measurements.Application.Responses.EnumParsers;
using Energinet.DataHub.Measurements.Domain;
using Xunit;
using Xunit.Categories;

namespace Energinet.DataHub.Measurements.UnitTests.Application.Responses.EnumParserTests;

[UnitTest]
public class QualityParserTests
{
    [Theory]
    [InlineData("missing", Quality.Missing)]
    [InlineData("estimated", Quality.Estimated)]
    [InlineData("measured", Quality.Measured)]
    [InlineData("calculated", Quality.Calculated)]
    public void ParseQuality_ValidInput_ReturnsExpectedQuality(string input, Quality expected)
    {
        // Act
        var result = QualityParser.ParseQuality(input);

        // Assert
        Assert.Equal(expected, result);
    }

    [Fact]
    public void ParseQuality_InvalidInput_ThrowsArgumentOutOfRangeException()
    {
        // Arrange
        const string invalidInput = "invalid_quality";

        // Act & Assert
        Assert.Throws<ArgumentOutOfRangeException>(() => QualityParser.ParseQuality(invalidInput));
    }
}

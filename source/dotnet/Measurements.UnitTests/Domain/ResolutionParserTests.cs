using Energinet.DataHub.Measurements.Domain;
using Xunit;

namespace Energinet.DataHub.Measurements.UnitTests.Domain;

public class ResolutionParserTests
{
    [Theory]
    [InlineData("PT15M", Resolution.PT15M)]
    [InlineData("PT1H", Resolution.PT1H)]
    [InlineData("P1D", Resolution.P1D)]
    [InlineData("P1M", Resolution.P1M)]
    [InlineData("P1Y", Resolution.P1Y)]
    public void ParseResolution_ValidInput_ReturnsExpectedResolution(string input, Resolution expected)
    {
        // Act
        var result = ResolutionParser.ParseResolution(input);

        // Assert
        Assert.Equal(expected, result);
    }

    [Fact]
    public void ParseResolution_InvalidInput_ThrowsArgumentOutOfRangeException()
    {
        // Arrange
        const string invalidInput = "invalid_resolution";

        // Act & Assert
        Assert.Throws<ArgumentOutOfRangeException>(() => ResolutionParser.ParseResolution(invalidInput));
    }
}

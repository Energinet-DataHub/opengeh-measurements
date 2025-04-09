using Energinet.DataHub.Measurements.Application.Responses;
using Energinet.DataHub.Measurements.Domain;
using Xunit;

namespace Energinet.DataHub.Measurements.UnitTests.Application.Responses;

public class ResolutionParserTests
{
    [Theory]
    [InlineData("PT15M", Resolution.QuarterHourly)]
    [InlineData("PT1H", Resolution.Hourly)]
    [InlineData("P1D", Resolution.Daily)]
    [InlineData("P1M", Resolution.Monthly)]
    [InlineData("P1Y", Resolution.Yearly)]
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

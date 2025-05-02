using Energinet.DataHub.Measurements.Application.Responses.EnumParsers;
using Energinet.DataHub.Measurements.Domain;
using Xunit;
using Xunit.Categories;

namespace Energinet.DataHub.Measurements.UnitTests.Application.Responses.EnumParserTests;

[UnitTest]
public class UnitParserTests
{
    [Theory]
    [InlineData("kwh", Unit.kWh)]
    [InlineData("KWH", Unit.kWh)]
    [InlineData("kWh", Unit.kWh)]
    [InlineData("kw", Unit.kW)]
    [InlineData("KW", Unit.kW)]
    [InlineData("mw", Unit.MW)]
    [InlineData("mwh", Unit.MWh)]
    [InlineData("tonne", Unit.Tonne)]
    [InlineData("kvarh", Unit.kVArh)]
    [InlineData("mvar", Unit.MVAr)]
    public void ParseUnit_ValidInput_ReturnsExpectedUnit(string input, Unit expected)
    {
        // Act
        var result = UnitParser.ParseUnit(input);

        // Assert
        Assert.Equal(expected, result);
    }

    [Fact]
    public void ParseUnit_InvalidInput_ThrowsArgumentOutOfRangeException()
    {
        // Arrange
        const string invalidInput = "invalid_resolution";

        // Act & Assert
        Assert.Throws<ArgumentOutOfRangeException>(() => UnitParser.ParseUnit(invalidInput));
    }
}

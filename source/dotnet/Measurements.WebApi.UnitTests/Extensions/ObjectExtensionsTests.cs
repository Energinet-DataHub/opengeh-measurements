using Energinet.DataHub.Measurements.WebApi.Extensions;

namespace Energinet.DataHub.Measurements.WebApi.UnitTests.Extensions
{
    public class ObjectExtensionsTests
    {
        [Theory]
        [InlineData("Hello\nWorld", "HelloWorld")]
        [InlineData("Line1\r\nLine2", "Line1Line2")]
        [InlineData("Line1\n\n\nLine2", "Line1Line2")]
        [InlineData("NoNewLine", "NoNewLine")]
        [InlineData("  Trimmed  ", "Trimmed")]
        [InlineData("\n\r", "")]
        [InlineData("", "")]
        [InlineData(null, "")]
        public void ToSanitizedString_RemovesNewLinesAndHandlesNullOrEmpty(string? input, string expected)
        {
            // Act
            var result = input.ToSanitizedString();

            // Assert
            Assert.Equal(expected, result);
        }

        [Fact]
        public void ToSanitizedString_WithNonStringObject_CallsToStringAndSanitizes()
        {
            // Arrange
            var number = 12345;

            // Act
            var result = number.ToSanitizedString();

            // Assert
            Assert.Equal("12345", result);
        }

        [Fact]
        public void ToSanitizedString_WithCustomObject_CallsToStringAndSanitizes()
        {
            // Arrange
            var customObject = new TestClass();

            // Act
            var result = customObject.ToSanitizedString();

            // Assert
            Assert.Equal("MyCustomStringWithNewLine", result);
        }

        private class TestClass
        {
            public override string ToString()
            {
                return "MyCustomString\nWithNewLine";
            }
        }
    }
}

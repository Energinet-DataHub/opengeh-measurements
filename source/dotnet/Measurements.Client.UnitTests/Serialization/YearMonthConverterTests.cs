using System.Text;
using System.Text.Json;
using Energinet.DataHub.Measurements.Client.Serialization;
using NodaTime;

namespace Energinet.DataHub.Measurements.Client.UnitTests.Serialization;

public class YearMonthConverterTests
{
    [Fact]
    public void Write_WhenValidYearMonth_ThenConvertedToCorrectString()
    {
        // Arrange
        var yearMonth = new YearMonth(2025, 10);
        var options = new JsonSerializerOptions();
        var memoryStream = new MemoryStream();
        var writer = new Utf8JsonWriter(memoryStream);

        // Act
        var converter = new YearMonthConverter();
        converter.Write(writer, yearMonth, options);
        writer.Flush();

        // Assert
        var actual = Encoding.UTF8.GetString(memoryStream.ToArray());
        Assert.Equal("\"2025-10\"", actual);
    }

    [Fact]
    public void Read_WhenValidYearMonthInJson_ShouldReadCorrectFormat()
    {
        // Arrange
        const string json = "\"2025-10\"";
        var options = new JsonSerializerOptions();
        var reader = new Utf8JsonReader(Encoding.UTF8.GetBytes(json));

        // Act
        var converter = new YearMonthConverter();
        reader.Read();
        var actual = converter.Read(ref reader, typeof(YearMonth), options);

        // Assert
        Assert.Equal(new YearMonth(2025, 10), actual);
    }
}

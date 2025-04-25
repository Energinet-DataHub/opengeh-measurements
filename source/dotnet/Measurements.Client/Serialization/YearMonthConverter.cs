using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using NodaTime;
using NodaTime.Text;

namespace Energinet.DataHub.Measurements.Client.Serialization;

public class YearMonthConverter : JsonConverter<YearMonth>
{
    public override YearMonth Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) //TODO: add tests
    {
        var value = reader.GetString() ?? throw new JsonException("YearMonth value cannot be null.");
        return YearMonthPattern.Iso.Parse(value).Value;
    }

    public override void Write(Utf8JsonWriter writer, YearMonth value, JsonSerializerOptions options)
    {
        writer.WriteStringValue(value.ToString("yyyy-MM", CultureInfo.InvariantCulture));
    }
}

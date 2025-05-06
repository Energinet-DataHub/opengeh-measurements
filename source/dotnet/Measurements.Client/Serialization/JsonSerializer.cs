using System.Text.Json;
using System.Text.Json.Serialization;
using NodaTime.Serialization.SystemTextJson;

namespace Energinet.DataHub.Measurements.Client.Serialization;

public class JsonSerializer
{
    public JsonSerializerOptions Options { get; } = new()
    {
        PropertyNameCaseInsensitive = true,
        Converters =
        {
            NodaConverters.InstantConverter,
            new JsonStringEnumConverter(),
            new YearMonthConverter(),
        },
    };

    public T Deserialize<T>(string value)
    {
        return System.Text.Json.JsonSerializer.Deserialize<T>(value, Options)!;
    }

    public T Deserialize<T>(JsonElement jsonElement)
    {
        return jsonElement.Deserialize<T>(Options) ?? throw new InvalidOperationException();
    }
}

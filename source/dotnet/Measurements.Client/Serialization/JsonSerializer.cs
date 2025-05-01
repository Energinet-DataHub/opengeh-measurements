using System.Text.Json;
using System.Text.Json.Serialization;
using NodaTime.Serialization.SystemTextJson;

namespace Energinet.DataHub.Measurements.Client.Serialization;

public class JsonSerializer : IJsonSerializer
{
    private readonly JsonSerializerOptions _options = new()
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
        return System.Text.Json.JsonSerializer.Deserialize<T>(value, _options)!;
    }

    public T Deserialize<T>(JsonElement jsonElement)
    {
        return jsonElement.Deserialize<T>(_options) ?? throw new InvalidOperationException();
    }
}

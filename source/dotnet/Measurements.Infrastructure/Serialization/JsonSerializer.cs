using System.Text.Json;
using System.Text.Json.Serialization;

namespace Energinet.DataHub.Measurements.Infrastructure.Serialization;

public class JsonSerializer
{
    private readonly JsonSerializerOptions _options = new()
    {
        PropertyNameCaseInsensitive = true,
        IncludeFields = false,
        Converters = { new JsonStringEnumConverter() },
    };

    public T Deserialize<T>(string value)
    {
        return System.Text.Json.JsonSerializer.Deserialize<T>(value, _options)!;
    }
}

using System.Text.Json;

namespace Energinet.DataHub.Measurements.Infrastructure.Serialization;

public class JsonSerializer
{
    private readonly JsonSerializerOptions _options = new()
    {
        PropertyNameCaseInsensitive = true,
        IncludeFields = false,
    };

    public T Deserialize<T>(string value)
    {
        return System.Text.Json.JsonSerializer.Deserialize<T>(value, _options)!;
    }
}

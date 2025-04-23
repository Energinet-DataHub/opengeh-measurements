using System.Text.Json;
using System.Text.Json.Serialization;
using NodaTime.Serialization.SystemTextJson;

namespace Energinet.DataHub.Measurements.Client.Serialization;

public class MeasurementSerializer
{
    private readonly JsonSerializerOptions _options = new()
    {
        PropertyNameCaseInsensitive = true,
        Converters =
        {
            NodaConverters.InstantConverter,
            new JsonStringEnumConverter(),
        },
    };

    public T Deserialize<T>(string value)
    {
        return JsonSerializer.Deserialize<T>(value, _options)!;
    }
}

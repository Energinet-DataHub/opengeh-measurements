using System.Text.Json;
using System.Text.Json.Serialization;
using NodaTime.Serialization.SystemTextJson;

namespace Energinet.DataHub.Measurements.Client.Serialization;

public class MeasurementSerializer : IJsonSerializer
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
        return JsonSerializer.Deserialize<T>(value, _options)!;
    }
}

public interface IJsonSerializer
{
    T Deserialize<T>(string value);
}

using System.Text.Json;
using NodaTime.Serialization.SystemTextJson;

namespace Energinet.DataHub.Measurements.WebApi.IntegrationTests.Extensions;

public static class StringExtensions
{
    private static readonly JsonSerializerOptions _options = new()
    {
        PropertyNameCaseInsensitive = true,
        Converters = { NodaConverters.InstantConverter },
    };

    public static T DeserializeJson<T>(this string value)
    {
        return JsonSerializer.Deserialize<T>(value, _options)!;
    }
}

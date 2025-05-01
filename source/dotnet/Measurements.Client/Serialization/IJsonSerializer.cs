using System.Text.Json;

namespace Energinet.DataHub.Measurements.Client.Serialization;

/// <summary>
/// Interface for JSON serialization.
/// </summary>
public interface IJsonSerializer
{
    /// <summary>
    /// Serializes a string to value of specified type.
    /// </summary>
    T Deserialize<T>(string value);

    /// <summary>
    /// Deserializes a JsonElement to value of specified type.
    /// </summary>
    T Deserialize<T>(JsonElement jsonElement);
}

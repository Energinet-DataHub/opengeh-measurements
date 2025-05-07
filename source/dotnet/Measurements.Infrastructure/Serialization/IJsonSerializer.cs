namespace Energinet.DataHub.Measurements.Infrastructure.Serialization;

/// <summary>
/// Interface for JSON serialization.
/// </summary>
public interface IJsonSerializer
{
    /// <summary>
    /// Serializes a specified type to a string.
    /// </summary>
    string Serialize<T>(T value);

    /// <summary>
    /// Deserializes a string to value of specified type.
    /// </summary>
    T Deserialize<T>(string value);
}

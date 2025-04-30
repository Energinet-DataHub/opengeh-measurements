namespace Energinet.DataHub.Measurements.Infrastructure.Serialization;

/// <summary>
/// Interface for JSON serialization.
/// </summary>
public interface IJsonSerializer
{
    string Serialize<T>(T value);

    T Deserialize<T>(string value);
}

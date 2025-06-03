using Energinet.DataHub.Measurements.Client.Authentication;

namespace Energinet.DataHub.Measurements.Client.Extensions.Options;

public class MeasurementClientOptions
{
    /// <summary>
    /// Custom implementation of IAuthorizationHeaderProvider.
    /// </summary>
    public IAuthorizationHeaderProvider? AuthorizationHeaderProvider { get; set; }
}

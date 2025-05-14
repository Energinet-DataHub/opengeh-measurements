using Energinet.DataHub.Measurements.Abstractions.Api.Models;

namespace Energinet.DataHub.Measurements.Client.ResponseParsers;

public interface IMeasurementsForDateResponseParser
{
    Task<MeasurementDto> ParseResponseMessage(HttpResponseMessage response, CancellationToken cancellationToken);
}

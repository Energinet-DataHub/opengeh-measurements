using Energinet.DataHub.Measurements.Abstractions.Api.Models;

namespace Energinet.DataHub.Measurements.Client.ResponseParsers;

public interface IMeasurementsForDayResponseParser
{
    Task<MeasurementDto> ParseResponseMessage(HttpResponseMessage response, CancellationToken cancellationToken);
}

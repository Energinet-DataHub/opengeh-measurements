using System.Collections.ObjectModel;
using Energinet.DataHub.Measurements.Abstractions.Api.Models;

namespace Energinet.DataHub.Measurements.Client.ResponseParsers;

public interface IMeasurementsForPeriodResponseParser
{
    Task<ReadOnlyCollection<MeasurementPointDto>> ParseResponseMessage(HttpResponseMessage response, CancellationToken cancellationToken);
}

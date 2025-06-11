using System.Collections.ObjectModel;
using Energinet.DataHub.Measurements.Abstractions.Api.Models;

namespace Energinet.DataHub.Measurements.Client.ResponseParsers;

public class MeasurementsForPeriodResponseParser : IMeasurementsForPeriodResponseParser
{
    public async Task<ReadOnlyCollection<MeasurementPointDto>> ParseResponseMessage(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        return await Task.FromResult(new ReadOnlyCollection<MeasurementPointDto>([]));
    }
}

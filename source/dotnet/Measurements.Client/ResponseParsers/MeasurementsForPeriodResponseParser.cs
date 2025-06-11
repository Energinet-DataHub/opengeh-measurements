using System.Collections.ObjectModel;
using Energinet.DataHub.Measurements.Abstractions.Api.Models;
using Energinet.DataHub.Measurements.Client.Serialization;

namespace Energinet.DataHub.Measurements.Client.ResponseParsers;

public class MeasurementsForPeriodResponseParser : IMeasurementsForPeriodResponseParser
{
    public async Task<ReadOnlyCollection<MeasurementPointDto>> ParseResponseMessage(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        var json = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        var pointsDto = new JsonSerializer().Deserialize<PointsDto>(json);
        var measurementPointDtos = CreatePoints(pointsDto.Points);

        return new ReadOnlyCollection<MeasurementPointDto>(measurementPointDtos);
    }

    private static List<MeasurementPointDto> CreatePoints(List<PointDto> pointDtos)
    {
        return pointDtos
            .Select((point, pointIndex) =>
                new MeasurementPointDto(
                    Order: pointIndex + 1,
                    point.Quantity,
                    point.Quality,
                    point.Unit,
                    point.Resolution,
                    point.Created,
                    point.TransactionCreated))
            .ToList();
    }

    private record PointsDto(List<PointDto> Points);

    private record PointDto(
        DateTimeOffset ObservationTime,
        decimal? Quantity,
        Quality Quality,
        Unit Unit,
        Resolution Resolution,
        DateTimeOffset Created,
        DateTimeOffset TransactionCreated);
}

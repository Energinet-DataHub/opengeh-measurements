using Energinet.DataHub.Measurements.Abstractions.Api.Models;
using Energinet.DataHub.Measurements.Client.Serialization;

namespace Energinet.DataHub.Measurements.Client.ResponseParsers;

public class MeasurementsForDayResponseParser : IMeasurementsForDayResponseParser
{
    public async Task<MeasurementDto> ParseResponseMessage(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        var measurementPositions = new List<MeasurementPositionDto>();

        var json = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        var pointsDto = new JsonSerializer().Deserialize<PointsDto>(json);

        var positions = pointsDto.Points
            .OrderBy(p => p.ObservationTime)
            .GroupBy(p => p.ObservationTime)
            .ToList();

        for (var positionIndex = 0; positionIndex < positions.Count; positionIndex++)
        {
            var position = positions[positionIndex];
            var points = CreatePoints(position);

            measurementPositions.Add(
                new MeasurementPositionDto(
                    Index: positionIndex + 1,
                    position.Key,
                    points));
        }

        return new MeasurementDto(measurementPositions);
    }

    private static List<MeasurementPointDto> CreatePoints(IGrouping<DateTimeOffset, PointDto> position)
    {
        var orderedPoints = position.OrderByDescending(p => p.TransactionCreated).ToList();

        return orderedPoints
            .Select((_, pointIndex) => orderedPoints.ElementAt(pointIndex))
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
        decimal Quantity,
        Quality Quality,
        Unit Unit,
        Resolution Resolution,
        DateTimeOffset Created,
        DateTimeOffset TransactionCreated);
}

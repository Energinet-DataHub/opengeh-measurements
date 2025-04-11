using Energinet.DataHub.Measurements.Abstractions.Api.Models;
using Energinet.DataHub.Measurements.Client.Serialization;

namespace Energinet.DataHub.Measurements.Client.ResponseParsers;

public class MeasurementsForDayResponseParser : IMeasurementsForDayResponseParser
{
    public async Task<MeasurementDto> ParseResponseMessage(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        var measurementPositions = new List<MeasurementPositionDto>();

        var json = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        var pointsDto = new MeasurementSerializer().Deserialize<PointsDto>(json);

        var positions = pointsDto.Points.GroupBy(p => p.ObservationTime).ToList();

        for (var positionIndex = 0; positionIndex < positions.Count; positionIndex++)
        {
            var position = positions[positionIndex];
            var orderedPoints = position.OrderByDescending(p => p.TransactionCreated).ToList();
            var points = new List<MeasurementPointDto>();

            for (var pointIndex = 0; pointIndex < orderedPoints.Count; pointIndex++)
            {
                var point = orderedPoints.ElementAt(pointIndex);

                points.Add(new MeasurementPointDto(
                    pointIndex + 1,
                    point.Quantity,
                    point.Quality,
                    point.Unit,
                    point.Resolution,
                    point.Created));
            }

            measurementPositions.Add(
                new MeasurementPositionDto(
                    positionIndex + 1,
                    position.Key,
                    points));
        }

        return new MeasurementDto(measurementPositions);
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

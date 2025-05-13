using System.Globalization;
using NodaTime;

namespace Energinet.DataHub.Measurements.WebApi.IntegrationTests.Fixtures;

public class MeasurementsTableRowsBuilder
{
    private readonly List<List<string>> _rows = [];

    public MeasurementsTableRowsBuilder WithRow(List<string> row)
    {
        _rows.Add(row);
        return this;
    }

    public MeasurementsTableRowsBuilder WithContinuousRows(string meteringPointId, LocalDate startObservationDate, int numberOfObservations)
    {
        for (var i = 0; i < numberOfObservations; i++)
        {
            var observationTime = Instant
                .FromUtc(startObservationDate.Year, startObservationDate.Month, startObservationDate.Day, i, 0, 0)
                .Plus(Duration.FromHours(-1));

            var rowBuilder = new MeasurementTableRowBuilder();
            var row = rowBuilder
                .WithMeteringPointId(meteringPointId)
                .WithObservationTime(FormatString(observationTime))
                .WithCreated(FormatString(Instant.FromUtc(startObservationDate.Year, startObservationDate.Month, startObservationDate.Day, 23, 0, 0)))
                .Build();

            _rows.Add(row);
        }

        return this;
    }

    public MeasurementsTableRowsBuilder WithContinuousRowsForDay(string meteringPointId, LocalDate startObservationDate)
    {
        return WithContinuousRows(meteringPointId, startObservationDate, 24);
    }

    public List<List<string>> Build()
    {
        return _rows;
    }

    private static string FormatString(Instant date)
    {
        return date.ToString("yyyy-MM-ddTHH:mm:ss'Z'", CultureInfo.InvariantCulture);
    }
}

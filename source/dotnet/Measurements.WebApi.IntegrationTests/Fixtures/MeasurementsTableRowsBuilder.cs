using System.Globalization;
using NodaTime;

namespace Energinet.DataHub.Measurements.WebApi.IntegrationTests.Fixtures;

public class MeasurementsTableRowsBuilder
{
    private readonly List<List<string>> _measurements = [];

    public MeasurementsTableRowsBuilder WithRow(List<string> row)
    {
        _measurements.Add(row);
        return this;
    }

    public MeasurementsTableRowsBuilder WithContinuesRowsForDay(string meteringPointId, LocalDate observationDate)
    {
        for (var i = 0; i < 24; i++)
        {
            var observationTime = Instant
                .FromUtc(observationDate.Year, observationDate.Month, observationDate.Day, i, 0, 0)
                .Plus(Duration.FromHours(-1));

            var rowBuilder = new MeasurementTableRowBuilder();
            var row = rowBuilder
                .WithMeteringPointId(meteringPointId)
                .WithObservationTime(FormatString(observationTime))
                .WithCreated(FormatString(Instant.FromUtc(observationDate.Year, observationDate.Month, observationDate.Day, 23, 0, 0)))
                .Build();

            _measurements.Add(row);
        }

        return this;
    }

    public List<List<string>> Build()
    {
        return _measurements;
    }

    private static string FormatString(Instant date)
    {
        return date.ToString("yyyy-MM-ddTHH:mm:ss'Z'", CultureInfo.InvariantCulture);
    }
}

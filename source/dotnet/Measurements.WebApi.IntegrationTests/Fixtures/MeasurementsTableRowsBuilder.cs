using System.Globalization;
using Energinet.DataHub.Measurements.Infrastructure.Extensions;
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

    public MeasurementsTableRowsBuilder WithContinuousRows(string meteringPointId, LocalDate observationDate, int numberOfObservations)
    {
        var startObservationTime = observationDate.ToInstantAtMidnight();

        for (var i = 0; i < numberOfObservations; i++)
        {
            var observationTime = startObservationTime.Plus(Duration.FromHours(i));

            var rowBuilder = new MeasurementTableRowBuilder();
            var row = rowBuilder
                .WithMeteringPointId(meteringPointId)
                .WithObservationTime(FormatString(observationTime))
                .WithCreated(FormatString(startObservationTime.Plus(Duration.FromDays(5))))
                .Build();

            _rows.Add(row);
        }

        return this;
    }

    public MeasurementsTableRowsBuilder WithContinuousRowsForDate(string meteringPointId, LocalDate startObservationDate)
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

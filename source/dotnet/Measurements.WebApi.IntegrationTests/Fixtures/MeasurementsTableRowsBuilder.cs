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

    public MeasurementsTableRowsBuilder WithContinuesRowsForDate(string meteringPointId, LocalDate observationDate)
    {
        var startObservationTime = observationDate.ToInstantAtMidnight();

        for (var i = 0; i < 24; i++)
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

    public List<List<string>> Build()
    {
        return _rows;
    }

    private static string FormatString(Instant date)
    {
        return date.ToString("yyyy-MM-ddTHH:mm:ss'Z'", CultureInfo.InvariantCulture);
    }
}

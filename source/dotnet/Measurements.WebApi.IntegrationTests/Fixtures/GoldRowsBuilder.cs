using NodaTime;

namespace Energinet.DataHub.Measurements.WebApi.IntegrationTests.Fixtures;

public class GoldRowsBuilder
{
    private readonly List<List<string>> _rows = [];

    public GoldRowsBuilder WithRow(List<string> row)
    {
        _rows.Add(row);
        return this;
    }

    public GoldRowsBuilder WithContinuesRowsForDate(string meteringPointId, LocalDate observationDate)
    {
        for (var i = 0; i < 24; i++)
        {
            var observationTime = Instant.FromUtc(observationDate.Year, observationDate.Month, observationDate.Day, i, 0, 0).Plus(Duration.FromHours(-1));
            var transactionCreated = Instant.FromUtc(observationDate.Year, observationDate.Month, observationDate.Day, 0, 0, 0).Plus(Duration.FromHours(-1));

            var rowBuilder = new GoldRowBuilder();
            var row = rowBuilder
                .WithMeteringPointId(meteringPointId)
                .WithObservationTime(observationTime)
                .WithCreated(transactionCreated)
                .WithTransactionCreationDatetime(transactionCreated)
                .Build();

            _rows.Add(row);
        }

        return this;
    }

    public List<List<string>> Build()
    {
        return _rows;
    }
}

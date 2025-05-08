using System.Globalization;
using NodaTime;

namespace Energinet.DataHub.Measurements.WebApi.IntegrationTests.Fixtures;

public class GoldRowBuilder
{
    private string _meteringPointId = "123456789012345678";
    private string _unit = "kwh";
    private Instant _observationTime;
    private string _quantity = "1.0";
    private string _quality = "measured";
    private string _resolution = "PT1H";
    private string _isCancelled = "false";
    private Instant _created;
    private Instant _transactionCreationDatetime;

    public GoldRowBuilder WithMeteringPointId(string meteringPointId)
    {
        _meteringPointId = meteringPointId;
        return this;
    }

    public GoldRowBuilder WithUnit(string unit)
    {
        _unit = unit;
        return this;
    }

    public GoldRowBuilder WithObservationTime(Instant observationTime)
    {
        _observationTime = observationTime;
        return this;
    }

    public GoldRowBuilder WithQuantity(string quantity)
    {
        _quantity = quantity;
        return this;
    }

    public GoldRowBuilder WithQuality(string quality)
    {
        _quality = quality;
        return this;
    }

    public GoldRowBuilder WithResolution(string resolution)
    {
        _resolution = resolution;
        return this;
    }

    public GoldRowBuilder WithIsCancelled(string isCancelled)
    {
        _isCancelled = isCancelled;
        return this;
    }

    public GoldRowBuilder WithCreated(Instant created)
    {
        _created = created;
        return this;
    }

    public GoldRowBuilder WithTransactionCreationDatetime(Instant transactionCreationDatetime)
    {
        _transactionCreationDatetime = transactionCreationDatetime;
        return this;
    }

    public List<string> Build()
    {
        return
        [
            $"'{_meteringPointId}'",
            $"'{_unit}'",
            $"'{FormatString(_observationTime)}'",
            _quantity,
            $"'{_quality}'",
            $"'{_resolution}'",
            _isCancelled,
            $"'{FormatString(_created)}'",
            $"'{FormatString(_transactionCreationDatetime)}'",
        ];
    }

    private static string FormatString(Instant date)
    {
        return date.ToString("yyyy-MM-ddTHH:mm:ss'Z'", CultureInfo.InvariantCulture);
    }
}

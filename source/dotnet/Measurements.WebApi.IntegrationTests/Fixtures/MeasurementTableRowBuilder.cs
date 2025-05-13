namespace Energinet.DataHub.Measurements.WebApi.IntegrationTests.Fixtures;

public class MeasurementTableRowBuilder
{
    private string _meteringPointId = string.Empty;
    private string _unit = "kwh";
    private string _observationTime = "1970-01-01T00:00:00Z";
    private string _quantity = "1.0";
    private string _quality = "measured";
    private string _resolution = "PT1H";
    private string _isCancelled = "false";
    private string _created = "1970-01-01T00:00:00Z";
    private string _transactionCreationDatetime = "1970-01-01T00:00:00Z";

    public MeasurementTableRowBuilder WithMeteringPointId(string meteringPointId)
    {
        _meteringPointId = meteringPointId;
        return this;
    }

    public MeasurementTableRowBuilder WithUnit(string unit)
    {
        _unit = unit;
        return this;
    }

    public MeasurementTableRowBuilder WithObservationTime(string observationTime)
    {
        _observationTime = observationTime;
        return this;
    }

    public MeasurementTableRowBuilder WithQuantity(string quantity)
    {
        _quantity = quantity;
        return this;
    }

    public MeasurementTableRowBuilder WithQuality(string quality)
    {
        _quality = quality;
        return this;
    }

    public MeasurementTableRowBuilder WithResolution(string resolution)
    {
        _resolution = resolution;
        return this;
    }

    public MeasurementTableRowBuilder WithIsCancelled(string isCancelled)
    {
        _isCancelled = isCancelled;
        return this;
    }

    public MeasurementTableRowBuilder WithCreated(string created)
    {
        _created = created;
        return this;
    }

    public MeasurementTableRowBuilder WithTransactionCreationDatetime(string transactionCreationDatetime)
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
            $"'{_observationTime}'",
            _quantity,
            $"'{_quality}'",
            $"'{_resolution}'",
            _isCancelled,
            $"'{_created}'",
            $"'{(_transactionCreationDatetime == string.Empty ? _created : _transactionCreationDatetime)}'",
        ];
    }
}

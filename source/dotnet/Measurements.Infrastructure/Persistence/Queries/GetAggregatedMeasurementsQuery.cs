using Energinet.DataHub.Core.Databricks.SqlStatementExecution;
using Energinet.DataHub.Measurements.Application.Extensions.Options;
using NodaTime;

namespace Energinet.DataHub.Measurements.Infrastructure.Persistence.Queries;

public class GetAggregatedMeasurementsQuery : DatabricksStatement
{
    private readonly string _meteringPointId;
    private readonly YearMonth _yearMonth;
    private readonly DatabricksSchemaOptions _databricksSchemaOptions;

    public GetAggregatedMeasurementsQuery(string meteringPointId, YearMonth yearMonth, DatabricksSchemaOptions databricksSchemaOptions)
    {
        _meteringPointId = meteringPointId;
        _yearMonth = yearMonth;
        _databricksSchemaOptions = databricksSchemaOptions;
    }

    protected override string GetSqlStatement()
    {
        var (startDate, endDate) = _yearMonth.ToDateInterval();

        return
            $"with most_recent as (" +
            $"select row_number() over (partition by {MeasurementsGoldConstants.MeteringPointIdColumnName}, {MeasurementsGoldConstants.ObservationTimeColumnName} order by {MeasurementsGoldConstants.TransactionCreationDatetimeColumnName} desc) as row, " +
            $"{MeasurementsGoldConstants.MeteringPointIdColumnName}, {MeasurementsGoldConstants.UnitColumnName}, {MeasurementsGoldConstants.ObservationTimeColumnName}, {MeasurementsGoldConstants.QuantityColumnName}, {MeasurementsGoldConstants.QualityColumnName}, {MeasurementsGoldConstants.IsCancelledColumnName}" +
            $"from {_databricksSchemaOptions.CatalogName}.{_databricksSchemaOptions.SchemaName}.{MeasurementsGoldConstants.TableName} " +
            $"where {MeasurementsGoldConstants.MeteringPointIdColumnName} = '{_meteringPointId}' " +
            $"and {MeasurementsGoldConstants.ObservationTimeColumnName} >= '{startDate}' " +
            $"and {MeasurementsGoldConstants.ObservationTimeColumnName} < '{endDate}' " +
            $") " +
            $"select {MeasurementsGoldConstants.MeteringPointIdColumnName}, {MeasurementsGoldConstants.UnitColumnName}, {MeasurementsGoldConstants.ObservationTimeColumnName}, {MeasurementsGoldConstants.QuantityColumnName}, {MeasurementsGoldConstants.QualityColumnName} " +
            $"from most_recent " +
            $"where row = 1 " +
            $"and {MeasurementsGoldConstants.IsCancelledColumnName} is false " +
            $"order by {MeasurementsGoldConstants.ObservationTimeColumnName}";
    }
}

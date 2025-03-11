using Energinet.DataHub.Core.Databricks.SqlStatementExecution;
using Energinet.DataHub.Measurements.Application.Extensions.Options;
using NodaTime;

namespace Energinet.DataHub.Measurements.Infrastructure.Persistence.Queries;

public class GetMeasurementsQuery : DatabricksStatement
{
    private readonly string _meteringPointId;
    private readonly Instant _startDate;
    private readonly Instant _endDate;
    private readonly DatabricksSchemaOptions _databricksSchemaOptions;

    public GetMeasurementsQuery(string meteringPointId, Instant startDate, Instant endDate, DatabricksSchemaOptions databricksSchemaOptions)
    {
        _meteringPointId = meteringPointId;
        _startDate = startDate;
        _endDate = endDate;
        _databricksSchemaOptions = databricksSchemaOptions;
    }

    protected override string GetSqlStatement()
    {
        return
            $"select {MeasurementsCatalogConstants.MeteringPointIdColumnName}, {MeasurementsCatalogConstants.ObservationTimeColumnName}, {MeasurementsCatalogConstants.QuantityColumnName}, {MeasurementsCatalogConstants.QualityColumnName} " +
            $"from {_databricksSchemaOptions.CatalogName}.{_databricksSchemaOptions.SchemaName}.{MeasurementsCatalogConstants.TableName} " +
            $"where {MeasurementsCatalogConstants.MeteringPointIdColumnName} = '{_meteringPointId}' " +
            $"and {MeasurementsCatalogConstants.ObservationTimeColumnName} >= '{_startDate}' " +
            $"and {MeasurementsCatalogConstants.ObservationTimeColumnName} < '{_endDate}' " +
            $"order by {MeasurementsCatalogConstants.ObservationTimeColumnName}";
    }
}

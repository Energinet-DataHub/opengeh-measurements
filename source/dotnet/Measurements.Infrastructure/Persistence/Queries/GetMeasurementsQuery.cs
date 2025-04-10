using Energinet.DataHub.Core.Databricks.SqlStatementExecution;
using Energinet.DataHub.Measurements.Application.Extensions.Options;
using Energinet.DataHub.Measurements.Infrastructure.Extensions;
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
            $"with most_recent as (" +
            $"select row_number() over (partition by {MeasurementsGoldConstants.MeteringPointIdColumnName}, {MeasurementsGoldConstants.ObservationTimeColumnName} order by {MeasurementsGoldConstants.TransactionCreationDatetimeColumnName} desc) as row, " +
            $"{MeasurementsGoldConstants.MeteringPointIdColumnName}, {MeasurementsGoldConstants.UnitColumnName}, {MeasurementsGoldConstants.ObservationTimeColumnName}, {MeasurementsGoldConstants.QuantityColumnName}, {MeasurementsGoldConstants.QualityColumnName}, {MeasurementsGoldConstants.IsCancelledColumnName}, {MeasurementsGoldConstants.CreatedColumnName} " +
            $"from {_databricksSchemaOptions.CatalogName}.{_databricksSchemaOptions.SchemaName}.{MeasurementsGoldConstants.TableName} " +
            $"where {MeasurementsGoldConstants.MeteringPointIdColumnName} = :{QueryParameterConstants.MeteringPointIdParameter} " +
            $"and {MeasurementsGoldConstants.ObservationTimeColumnName} >= :{QueryParameterConstants.ObservationTimeFromParameter} " +
            $"and {MeasurementsGoldConstants.ObservationTimeColumnName} < :{QueryParameterConstants.ObservationTimeToParameter} " +
            $") " +
            $"select {MeasurementsGoldConstants.MeteringPointIdColumnName}, {MeasurementsGoldConstants.UnitColumnName}, {MeasurementsGoldConstants.ObservationTimeColumnName}, {MeasurementsGoldConstants.QuantityColumnName}, {MeasurementsGoldConstants.QualityColumnName}, {MeasurementsGoldConstants.CreatedColumnName} " +
            $"from most_recent " +
            $"where row = 1 " +
            $"and not {MeasurementsGoldConstants.IsCancelledColumnName} " +
            $"order by {MeasurementsGoldConstants.ObservationTimeColumnName}";
    }

    protected override IReadOnlyCollection<QueryParameter> GetParameters()
    {
        List<QueryParameter> parameters = [
            QueryParameter.Create(QueryParameterConstants.MeteringPointIdParameter, _meteringPointId),
            QueryParameter.Create(QueryParameterConstants.ObservationTimeFromParameter, _startDate.ToUtcString()),
            QueryParameter.Create(QueryParameterConstants.ObservationTimeToParameter, _endDate.ToUtcString())
        ];

        return parameters;
    }
}

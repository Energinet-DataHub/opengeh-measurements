using Energinet.DataHub.Core.Databricks.SqlStatementExecution;
using Energinet.DataHub.Measurements.Application.Extensions.Options;
using NodaTime;

namespace Energinet.DataHub.Measurements.Infrastructure.Persistence.Queries;

public class GetByPeriodQuery : DatabricksStatement
{
    private readonly string _meteringPointId;
    private readonly Instant _startDate;
    private readonly Instant _endDate;
    private readonly DatabricksSchemaOptions _databricksSchemaOptions;

    public GetByPeriodQuery(string meteringPointId, Instant startDate, Instant endDate, DatabricksSchemaOptions databricksSchemaOptions)
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
            $"select row_number() over (partition by {MeasurementsTableConstants.MeteringPointIdColumnName}, {MeasurementsTableConstants.ObservationTimeColumnName} order by {MeasurementsTableConstants.TransactionCreationDatetimeColumnName} desc) as row, " +
            $"{MeasurementsTableConstants.MeteringPointIdColumnName}, {MeasurementsTableConstants.UnitColumnName}, {MeasurementsTableConstants.ObservationTimeColumnName}, {MeasurementsTableConstants.QuantityColumnName}, {MeasurementsTableConstants.QualityColumnName}, {MeasurementsTableConstants.ResolutionColumnName}, {MeasurementsTableConstants.IsCancelledColumnName}, {MeasurementsTableConstants.CreatedColumnName}, {MeasurementsTableConstants.TransactionCreationDatetimeColumnName} " +
            $"from {_databricksSchemaOptions.CatalogName}.{_databricksSchemaOptions.SchemaName}.{MeasurementsTableConstants.Name} " +
            $"where {MeasurementsTableConstants.MeteringPointIdColumnName} = :{QueryParameterConstants.MeteringPointIdParameter} " +
            $"and {MeasurementsTableConstants.ObservationTimeColumnName} >= :{QueryParameterConstants.ObservationTimeFromParameter} " +
            $"and {MeasurementsTableConstants.ObservationTimeColumnName} < :{QueryParameterConstants.ObservationTimeToParameter} " +
            $") " +
            $"select {MeasurementsTableConstants.MeteringPointIdColumnName}, {MeasurementsTableConstants.UnitColumnName}, {MeasurementsTableConstants.ObservationTimeColumnName}, {MeasurementsTableConstants.QuantityColumnName}, {MeasurementsTableConstants.QualityColumnName}, {MeasurementsTableConstants.ResolutionColumnName}, {MeasurementsTableConstants.CreatedColumnName}, {MeasurementsTableConstants.TransactionCreationDatetimeColumnName} " +
            $"from most_recent " +
            $"where not {MeasurementsTableConstants.IsCancelledColumnName} " +
            $"order by {MeasurementsTableConstants.ObservationTimeColumnName}";
    }

    protected override IReadOnlyCollection<QueryParameter> GetParameters()
    {
        List<QueryParameter> parameters = [
            QueryParameter.Create(QueryParameterConstants.MeteringPointIdParameter, _meteringPointId),
            QueryParameter.Create(QueryParameterConstants.ObservationTimeFromParameter, _startDate.ToString()),
            QueryParameter.Create(QueryParameterConstants.ObservationTimeToParameter, _endDate.ToString())
        ];

        return parameters;
    }
}

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
            $"select {MeasurementsColumnNames.MeteringPointId}, {MeasurementsColumnNames.ObservationTime}, {MeasurementsColumnNames.Quantity}, {MeasurementsColumnNames.Quality}" +
            $"from {_databricksSchemaOptions.CatalogName}.{_databricksSchemaOptions.SchemaName}" +
            $"where {MeasurementsColumnNames.MeteringPointId} = {_meteringPointId}" +
            $"and {MeasurementsColumnNames.ObservationTime} >= {_startDate}" +
            $"and {MeasurementsColumnNames.ObservationTime} < {_endDate}" +
            $"order by {MeasurementsColumnNames.ObservationTime}";
    }
}

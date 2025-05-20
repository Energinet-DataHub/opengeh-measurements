using AutoFixture.Xunit2;
using Energinet.DataHub.Measurements.Application.Extensions.Options;
using Energinet.DataHub.Measurements.Infrastructure.Persistence;
using Energinet.DataHub.Measurements.Infrastructure.Persistence.Queries;
using Xunit;
using Xunit.Categories;

namespace Energinet.DataHub.Measurements.UnitTests.Infrastructure.Persistence.Queries;

[UnitTest]
public class GetAggregatedByYearQueryTests
{
    [Theory]
    [AutoData]
    public void ToString_Returns_ExpectedResult(string meteringPointId)
    {
        // Arrange
        var databricksSchemaOptions = new DatabricksSchemaOptions { CatalogName = "spark_catalog", SchemaName = "schema_name" };
        var expected = CreateExpectedQuery(databricksSchemaOptions);
        var getAggregatedByYearQuery = new GetAggregatedByYearQuery(meteringPointId, databricksSchemaOptions);

        // Act
        var actual = getAggregatedByYearQuery.ToString();

        // Assert
        Assert.Equal(expected, actual);
    }

    private static string CreateExpectedQuery(DatabricksSchemaOptions databricksSchemaOptions)
    {
        return $"with most_recent as (" +
               $"select row_number() over (partition by {MeasurementsTableConstants.MeteringPointIdColumnName}, {MeasurementsTableConstants.ObservationTimeColumnName} order by {MeasurementsTableConstants.TransactionCreationDatetimeColumnName} desc) as row, " +
               $"count(*) over (partition by {MeasurementsTableConstants.MeteringPointIdColumnName}, {MeasurementsTableConstants.ObservationTimeColumnName}) as row_count, " +
               $"{MeasurementsTableConstants.MeteringPointIdColumnName}, {MeasurementsTableConstants.UnitColumnName}, {MeasurementsTableConstants.ObservationTimeColumnName}, {MeasurementsTableConstants.QuantityColumnName}, {MeasurementsTableConstants.QualityColumnName}, {MeasurementsTableConstants.ResolutionColumnName}, {MeasurementsTableConstants.IsCancelledColumnName} " +
               $"from {databricksSchemaOptions.CatalogName}.{databricksSchemaOptions.SchemaName}.{MeasurementsTableConstants.Name} " +
               $"where {MeasurementsTableConstants.MeteringPointIdColumnName} = :{QueryParameterConstants.MeteringPointIdParameter} " +
               $") " +
               $"select {MeasurementsTableConstants.MeteringPointIdColumnName}, " +
               $"min({MeasurementsTableConstants.ObservationTimeColumnName}) as {AggregatedQueryConstants.MinObservationTime}, " +
               $"max({MeasurementsTableConstants.ObservationTimeColumnName}) as {AggregatedQueryConstants.MaxObservationTime}, " +
               $"sum({MeasurementsTableConstants.QuantityColumnName}) as {AggregatedQueryConstants.AggregatedQuantity}, " +
               $"array_agg(distinct({MeasurementsTableConstants.QualityColumnName})) as {AggregatedQueryConstants.Qualities}, " +
               $"array_agg(distinct({MeasurementsTableConstants.ResolutionColumnName})) as {AggregatedQueryConstants.Resolutions}, " +
               $"array_agg(distinct({MeasurementsTableConstants.UnitColumnName})) as {AggregatedQueryConstants.Units}, " +
               $"count({MeasurementsTableConstants.ObservationTimeColumnName}) as {AggregatedQueryConstants.PointCount}, " +
               $"max(row_count) as {AggregatedQueryConstants.ObservationUpdates} " +
               $"from most_recent " +
               $"where row = 1 " +
               $"and not {MeasurementsTableConstants.IsCancelledColumnName} " +
               $"group by {CreateGroupByStatement()} " +
               $"order by {AggregatedQueryConstants.MinObservationTime}";
    }

    private static string CreateGroupByStatement()
    {
        return $"{MeasurementsTableConstants.MeteringPointIdColumnName}" +
               $", year(from_utc_timestamp(cast({MeasurementsTableConstants.ObservationTimeColumnName} as timestamp), '{TimeZoneConstants.EuropeCopenhagenTimeZone}'))";
    }
}

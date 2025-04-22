using System.ComponentModel.DataAnnotations;
using AutoFixture.Xunit2;
using Energinet.DataHub.Measurements.Application.Extensions.Options;
using Energinet.DataHub.Measurements.Application.Requests;
using Energinet.DataHub.Measurements.Domain;
using Energinet.DataHub.Measurements.Infrastructure.Persistence;
using Energinet.DataHub.Measurements.Infrastructure.Persistence.Queries;
using NodaTime;
using Xunit;
using Xunit.Categories;

namespace Energinet.DataHub.Measurements.UnitTests.Infrastructure.Persistence.Queries;

[UnitTest]
public class GetAggregatedByYearQueryTests
{
    [Theory]
    [AutoData]
    public void ToString_Returns_ExpectedResult(string meteringPointId, [Range(-9998, 9999)] int testYear)
    {
        // Arrange
        var year = new Year(testYear);
        var databricksSchemaOptions = new DatabricksSchemaOptions { CatalogName = "spark_catalog", SchemaName = "schema_name" };
        var expected = CreateExpectedQuery(databricksSchemaOptions);
        var getAggregatedByYearQuery = new GetAggregatedByYearQuery(meteringPointId, year, databricksSchemaOptions);

        // Act
        var actual = getAggregatedByYearQuery.ToString();

        // Assert
        Assert.Equal(expected, actual);
    }

    private static string CreateExpectedQuery(DatabricksSchemaOptions databricksSchemaOptions)
    {
        return $"with most_recent as (" +
               $"select row_number() over (partition by {MeasurementsGoldConstants.MeteringPointIdColumnName}, {MeasurementsGoldConstants.ObservationTimeColumnName} order by {MeasurementsGoldConstants.TransactionCreationDatetimeColumnName} desc) as row, " +
               $"count(*) over (partition by {MeasurementsGoldConstants.MeteringPointIdColumnName}, {MeasurementsGoldConstants.ObservationTimeColumnName}) as row_count, " +
               $"{MeasurementsGoldConstants.MeteringPointIdColumnName}, {MeasurementsGoldConstants.UnitColumnName}, {MeasurementsGoldConstants.ObservationTimeColumnName}, {MeasurementsGoldConstants.QuantityColumnName}, {MeasurementsGoldConstants.QualityColumnName}, {MeasurementsGoldConstants.ResolutionColumnName}, {MeasurementsGoldConstants.IsCancelledColumnName} " +
               $"from {databricksSchemaOptions.CatalogName}.{databricksSchemaOptions.SchemaName}.{MeasurementsGoldConstants.TableName} " +
               $"where {MeasurementsGoldConstants.MeteringPointIdColumnName} = :{QueryParameterConstants.MeteringPointIdParameter} " +
               $"and {MeasurementsGoldConstants.ObservationTimeColumnName} >= :{QueryParameterConstants.ObservationTimeFromParameter} " +
               $"and {MeasurementsGoldConstants.ObservationTimeColumnName} < :{QueryParameterConstants.ObservationTimeToParameter} " +
               $") " +
               $"select {MeasurementsGoldConstants.MeteringPointIdColumnName}, " +
               $"min({MeasurementsGoldConstants.ObservationTimeColumnName}) as {AggregatedQueryConstants.MinObservationTime}, " +
               $"max({MeasurementsGoldConstants.ObservationTimeColumnName}) as {AggregatedQueryConstants.MaxObservationTime}, " +
               $"sum({MeasurementsGoldConstants.QuantityColumnName}) as {AggregatedQueryConstants.AggregatedQuantity}, " +
               $"array_agg(distinct({MeasurementsGoldConstants.QualityColumnName})) as {AggregatedQueryConstants.Qualities}, " +
               $"array_agg(distinct({MeasurementsGoldConstants.ResolutionColumnName})) as {AggregatedQueryConstants.Resolutions}, " +
               $"array_agg(distinct({MeasurementsGoldConstants.UnitColumnName})) as {AggregatedQueryConstants.Units}, " +
               $"count({MeasurementsGoldConstants.ObservationTimeColumnName}) as {AggregatedQueryConstants.PointCount}, " +
               $"max(row_count) as {AggregatedQueryConstants.ObservationUpdates} " +
               $"from most_recent " +
               $"where row = 1 " +
               $"and not {MeasurementsGoldConstants.IsCancelledColumnName} " +
               $"group by {CreateGroupByStatement()} " +
               $"order by {AggregatedQueryConstants.MinObservationTime}";
    }

    private static string CreateGroupByStatement()
    {
        const string europeCopenhagenTimeZone = "Europe/Copenhagen";

        return $"{MeasurementsGoldConstants.MeteringPointIdColumnName}" +
               $", year(from_utc_timestamp(cast({MeasurementsGoldConstants.ObservationTimeColumnName} as timestamp), '{europeCopenhagenTimeZone}'))" +
               $", month(from_utc_timestamp(cast({MeasurementsGoldConstants.ObservationTimeColumnName} as timestamp), '{europeCopenhagenTimeZone}'))";
    }
}

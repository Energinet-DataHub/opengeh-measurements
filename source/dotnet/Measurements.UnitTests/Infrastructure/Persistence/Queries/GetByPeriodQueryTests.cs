using AutoFixture.Xunit2;
using Energinet.DataHub.Measurements.Application.Extensions.Options;
using Energinet.DataHub.Measurements.Infrastructure.Persistence;
using Energinet.DataHub.Measurements.Infrastructure.Persistence.Queries;
using NodaTime;
using Xunit;
using Xunit.Categories;

namespace Energinet.DataHub.Measurements.UnitTests.Infrastructure.Persistence.Queries;

[UnitTest]
public class GetByPeriodQueryTests
{
    [Theory]
    [AutoData]
    public void ToString_Returns_ExpectedResult(string meteringPointId, Instant startDate, Instant endDate)
    {
        // Arrange
        var databricksSchemaOptions = new DatabricksSchemaOptions { CatalogName = "spark_catalog", SchemaName = "schema_name" };
        var expectedResult = CreateExpectedQuery(databricksSchemaOptions);
        var getByPeriodQuery = new GetByPeriodQuery(meteringPointId, startDate, endDate, databricksSchemaOptions);

        // Act
        var result = getByPeriodQuery.ToString();

        // Assert
        Assert.Equal(expectedResult, result);
    }

    private static string CreateExpectedQuery(DatabricksSchemaOptions databricksSchemaOptions)
    {
        return $"with most_recent as (" +
               $"select row_number() over (partition by {MeasurementsTableConstants.MeteringPointIdColumnName}, {MeasurementsTableConstants.ObservationTimeColumnName} order by {MeasurementsTableConstants.TransactionCreationDatetimeColumnName} desc) as row, " +
               $"{MeasurementsTableConstants.MeteringPointIdColumnName}, {MeasurementsTableConstants.UnitColumnName}, {MeasurementsTableConstants.ObservationTimeColumnName}, {MeasurementsTableConstants.QuantityColumnName}, {MeasurementsTableConstants.QualityColumnName}, {MeasurementsTableConstants.ResolutionColumnName}, {MeasurementsTableConstants.IsCancelledColumnName}, {MeasurementsTableConstants.CreatedColumnName}, {MeasurementsTableConstants.TransactionCreationDatetimeColumnName} " +
               $"from {databricksSchemaOptions.CatalogName}.{databricksSchemaOptions.SchemaName}.{MeasurementsTableConstants.Name} " +
               $"where {MeasurementsTableConstants.MeteringPointIdColumnName} = :{QueryParameterConstants.MeteringPointIdParameter} " +
               $"and {MeasurementsTableConstants.ObservationTimeColumnName} >= :{QueryParameterConstants.ObservationTimeFromParameter} " +
               $"and {MeasurementsTableConstants.ObservationTimeColumnName} < :{QueryParameterConstants.ObservationTimeToParameter} " +
               $") " +
               $"select {MeasurementsTableConstants.MeteringPointIdColumnName}, {MeasurementsTableConstants.UnitColumnName}, {MeasurementsTableConstants.ObservationTimeColumnName}, {MeasurementsTableConstants.QuantityColumnName}, {MeasurementsTableConstants.QualityColumnName}, {MeasurementsTableConstants.ResolutionColumnName}, {MeasurementsTableConstants.CreatedColumnName}, {MeasurementsTableConstants.TransactionCreationDatetimeColumnName} " +
               $"from most_recent " +
               $"where not {MeasurementsTableConstants.IsCancelledColumnName} " +
               $"order by {MeasurementsTableConstants.ObservationTimeColumnName}";
    }
}

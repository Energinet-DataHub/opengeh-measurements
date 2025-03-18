using Energinet.DataHub.Core.Databricks.SqlStatementExecution;
using Energinet.DataHub.Core.FunctionApp.TestCommon.Configuration;
using Energinet.DataHub.Core.FunctionApp.TestCommon.Databricks;
using Energinet.DataHub.Measurements.Application.Extensions.Options;
using Energinet.DataHub.Measurements.Infrastructure.Persistence;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Xunit;

namespace Energinet.DataHub.Measurements.WebApi.IntegrationTests.Fixtures;

/// <summary>
///     When we execute the tests on build agents we use the build output (assemblies).
///     To avoid an 'System.IO.DirectoryNotFoundException' exception from WebApplicationFactory
///     during creation, we must set the path to the 'content root' using an environment variable
///     named 'ASPNETCORE_TEST_CONTENTROOT_[ASSEMBLY_NAME]'. Where Assembly Name is separated with
///     '_' instead of '.', and in all caps. This is set inside the dotnet-postbuild-test.yml.
/// </summary>
public class WebApiFixture : WebApplicationFactory<Program>, IAsyncLifetime
{
    public WebApiFixture()
    {
        IntegrationTestConfiguration = new IntegrationTestConfiguration();
        DatabricksSchemaManager = new DatabricksSchemaManager(
            new HttpClientFactory(),
            IntegrationTestConfiguration.DatabricksSettings,
            "mmcore_measurementsapi");
    }

    public DatabricksSchemaManager DatabricksSchemaManager { get; set; }

    public IntegrationTestConfiguration IntegrationTestConfiguration { get; }

    public async Task InitializeAsync()
    {
        var columnDefinitions = CreateMeasurementsColumnDefinitions();
        var rows = CreateRows();

        await DatabricksSchemaManager.CreateSchemaAsync();
        await DatabricksSchemaManager.CreateTableAsync(MeasurementsGoldConstants.TableName, columnDefinitions);
        await DatabricksSchemaManager.InsertAsync(MeasurementsGoldConstants.TableName, rows);
    }

    public new async Task DisposeAsync()
    {
        await base.DisposeAsync();
        await DatabricksSchemaManager.DropSchemaAsync();
    }

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseSetting($"{DatabricksSqlStatementOptions.DatabricksOptions}:{nameof(DatabricksSqlStatementOptions.WorkspaceUrl)}", IntegrationTestConfiguration.DatabricksSettings.WorkspaceUrl);
        builder.UseSetting($"{DatabricksSqlStatementOptions.DatabricksOptions}:{nameof(DatabricksSqlStatementOptions.WorkspaceToken)}", IntegrationTestConfiguration.DatabricksSettings.WorkspaceAccessToken);
        builder.UseSetting($"{DatabricksSqlStatementOptions.DatabricksOptions}:{nameof(DatabricksSqlStatementOptions.WarehouseId)}", IntegrationTestConfiguration.DatabricksSettings.WarehouseId);
        builder.UseSetting($"{nameof(DatabricksSchemaOptions)}:{nameof(DatabricksSchemaOptions.SchemaName)}", DatabricksSchemaManager.SchemaName);
        builder.UseSetting($"{nameof(DatabricksSchemaOptions)}:{nameof(DatabricksSchemaOptions.CatalogName)}", "hive_metastore");
    }

    private static Dictionary<string, (string DataType, bool IsNullable)> CreateMeasurementsColumnDefinitions() =>
        new()
        {
            { MeasurementsGoldConstants.MeteringPointIdColumnName, ("STRING", false) },
            { MeasurementsGoldConstants.UnitColumnName, ("STRING", false) },
            { MeasurementsGoldConstants.ObservationTimeColumnName, ("TIMESTAMP", false) },
            { MeasurementsGoldConstants.QuantityColumnName, ("DECIMAL(18, 6)", false) },
            { MeasurementsGoldConstants.QualityColumnName, ("STRING", false) },
        };

    private static IEnumerable<IEnumerable<string>> CreateRows()
    {
        return Enumerable.Range(1, 24).Select(_ => new List<string> { "'1234567890'", "'kwh'", "'2022-01-01T00:00:00Z'", "1.0", "'measured'" });
    }
}

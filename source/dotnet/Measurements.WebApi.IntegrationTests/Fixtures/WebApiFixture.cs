using Energinet.DataHub.Core.Databricks.SqlStatementExecution;
using Energinet.DataHub.Core.FunctionApp.TestCommon.Configuration;
using Energinet.DataHub.Core.FunctionApp.TestCommon.Databricks;
using Energinet.DataHub.Measurements.Application.Extensions.Options;
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
        const string tableName = "measurements";
        var columnDefinitions = CreateMeasurementsColumnDefinitions();
        var columnNames = columnDefinitions.Keys.ToArray();
        var rows = CreateRows();

        await DatabricksSchemaManager.CreateSchemaAsync();
        await DatabricksSchemaManager.CreateTableAsync(tableName, columnDefinitions);
        await DatabricksSchemaManager.InsertAsync(tableName, rows);
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
            { "metering_point_id", ("STRING", false) },
            { "observation_time", ("TIMESTAMP", false) },
            { "quantity", ("DECIMAL(18, 6)", false) },
            { "quality", ("STRING", false) },
        };

    private static IEnumerable<IEnumerable<string>> CreateRows()
    {
        return Enumerable.Range(1, 24).Select(_ => new List<string> { "'1234567890'", "'2022-01-01T00:00:00Z'", "1.0", "'measured'" });

        // "'1234567890','2022-01-01T00:00:00Z',1.0,'measured'",
        // "'1234567890','2022-01-01T00:00:00Z',2.0,'measured'",
        // "'1234567890','2022-01-01T00:00:00Z',3.0,'measured'",
        // "'1234567890','2022-01-01T00:00:00Z',4.0,'measured'",
        // "'1234567890','2022-01-01T00:00:00Z',5.0,'measured'",
        // "'1234567890','2022-01-01T00:00:00Z',6.0,'measured'",
        // "'1234567890','2022-01-01T00:00:00Z',7.0,'measured'",
        // "'1234567890','2022-01-01T00:00:00Z',8.0,'measured'",
        // "'1234567890','2022-01-01T00:00:00Z',9.0,'measured'",
        // "'1234567890','2022-01-01T00:00:00Z',10.0,'measured'",
        // "'1234567890','2022-01-01T00:00:00Z',11.0,'measured'",
        // "'1234567890','2022-01-01T00:00:00Z',12.0,'measured'",
        // "'1234567890','2022-01-01T00:00:00Z',13.0,'measured'",
        // "'1234567890','2022-01-01T00:00:00Z',14.0,'measured'",
        // "'1234567890','2022-01-01T00:00:00Z',15.0,'measured'",
        // "'1234567890','2022-01-01T00:00:00Z',16.0,'measured'",
        // "'1234567890','2022-01-01T00:00:00Z',17.0,'measured'",
        // "'1234567890','2022-01-01T00:00:00Z',18.0,'measured'",
        // "'1234567890','2022-01-01T00:00:00Z',19.0,'measured'",
        // "'1234567890','2022-01-01T00:00:00Z',20.0,'measured'",
        // "'1234567890','2022-01-01T00:00:00Z',21.0,'measured'",
        // "'1234567890','2022-01-01T00:00:00Z',22.0,'measured'",
        // "'1234567890','2022-01-01T00:00:00Z',23.0,'measured'",
        // "'1234567890','2022-01-01T00:00:00Z',24.0,'measured'",
    }
}

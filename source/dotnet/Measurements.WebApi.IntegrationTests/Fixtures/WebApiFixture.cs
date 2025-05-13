using System.Net.Http.Headers;
using Azure.Core;
using Azure.Identity;
using Energinet.DataHub.Core.Databricks.SqlStatementExecution;
using Energinet.DataHub.Core.FunctionApp.TestCommon.Configuration;
using Energinet.DataHub.Core.FunctionApp.TestCommon.Databricks;
using Energinet.DataHub.Measurements.Application.Extensions.Options;
using Energinet.DataHub.Measurements.Infrastructure.Persistence;
using Microsoft.AspNetCore.Authentication.JwtBearer;
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
    private const string ApplicationIdUri = "https://management.azure.com";
    private const string Issuer = "https://sts.windows.net/f7619355-6c67-4100-9a78-1847f30742e2/";
    private const string CatalogName = "hive_metastore";

    public HttpClient Client { get; }

    public WebApiFixture()
    {
        IntegrationTestConfiguration = new IntegrationTestConfiguration();
        DatabricksSchemaManager = new DatabricksSchemaManager(
            new HttpClientFactory(),
            IntegrationTestConfiguration.DatabricksSettings,
            "mmcore_measurementsapi");
        Client = CreateClient();
        Client.DefaultRequestHeaders.Authorization = CreateAuthorizationHeader();
    }

    private DatabricksSchemaManager DatabricksSchemaManager { get; }

    private IntegrationTestConfiguration IntegrationTestConfiguration { get; }

    public async Task InitializeAsync()
    {
        await DatabricksSchemaManager.CreateSchemaAsync();
    }

    public new async Task DisposeAsync()
    {
        await base.DisposeAsync();
        await DatabricksSchemaManager.DropSchemaAsync();
    }

    public async Task CreateTableAsync()
    {
        var columnDefinitions = CreateMeasurementsColumnDefinitions();
        await DatabricksSchemaManager.CreateTableAsync(MeasurementsGoldConstants.TableName, columnDefinitions);
    }

    public async Task InsertRowsAsync(List<List<string>> rows)
    {
        await DatabricksSchemaManager.InsertAsync(MeasurementsGoldConstants.TableName, rows);
    }

    public async Task DeleteTableAsync()
    {
        await DatabricksSchemaManager.DropTableAsync(MeasurementsGoldConstants.TableName);
    }

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseSetting($"{DatabricksSqlStatementOptions.DatabricksOptions}:{nameof(DatabricksSqlStatementOptions.WorkspaceUrl)}", IntegrationTestConfiguration.DatabricksSettings.WorkspaceUrl);
        builder.UseSetting($"{DatabricksSqlStatementOptions.DatabricksOptions}:{nameof(DatabricksSqlStatementOptions.WorkspaceToken)}", IntegrationTestConfiguration.DatabricksSettings.WorkspaceAccessToken);
        builder.UseSetting($"{DatabricksSqlStatementOptions.DatabricksOptions}:{nameof(DatabricksSqlStatementOptions.WarehouseId)}", IntegrationTestConfiguration.DatabricksSettings.WarehouseId);
        builder.UseSetting($"{DatabricksSchemaOptions.SectionName}:{nameof(DatabricksSchemaOptions.SchemaName)}", DatabricksSchemaManager.SchemaName);
        builder.UseSetting($"{DatabricksSchemaOptions.SectionName}:{nameof(DatabricksSchemaOptions.CatalogName)}", CatalogName);
        builder.UseSetting($"{AuthenticationOptions.SectionName}:{nameof(AuthenticationOptions.ApplicationIdUri)}", ApplicationIdUri);
        builder.UseSetting($"{AuthenticationOptions.SectionName}:{nameof(AuthenticationOptions.Issuer)}", Issuer);
    }

    private AuthenticationHeaderValue CreateAuthorizationHeader(string applicationIdUri = ApplicationIdUri)
    {
        var token = new DefaultAzureCredential()
            .GetToken(new TokenRequestContext([applicationIdUri]), CancellationToken.None)
            .Token;

        return new AuthenticationHeaderValue(JwtBearerDefaults.AuthenticationScheme, token);
    }

    private static Dictionary<string, (string DataType, bool IsNullable)> CreateMeasurementsColumnDefinitions() =>
        new()
        {
            { MeasurementsGoldConstants.MeteringPointIdColumnName, ("STRING", false) },
            { MeasurementsGoldConstants.UnitColumnName, ("STRING", false) },
            { MeasurementsGoldConstants.ObservationTimeColumnName, ("TIMESTAMP", false) },
            { MeasurementsGoldConstants.QuantityColumnName, ("DECIMAL(18, 6)", false) },
            { MeasurementsGoldConstants.QualityColumnName, ("STRING", false) },
            { MeasurementsGoldConstants.ResolutionColumnName, ("STRING", false) },
            { MeasurementsGoldConstants.IsCancelledColumnName, ("BOOLEAN", true) },
            { MeasurementsGoldConstants.CreatedColumnName, ("TIMESTAMP", false) },
            { MeasurementsGoldConstants.TransactionCreationDatetimeColumnName, ("TIMESTAMP", false) },
        };
}

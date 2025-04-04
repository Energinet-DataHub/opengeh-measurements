using System.Globalization;
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
using NodaTime;
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
        var columnDefinitions = CreateMeasurementsColumnDefinitions();

        await DatabricksSchemaManager.CreateSchemaAsync();
        await DatabricksSchemaManager.CreateTableAsync(MeasurementsGoldConstants.TableName, columnDefinitions);
        await DatabricksSchemaManager.InsertAsync(MeasurementsGoldConstants.TableName, CreateRows());
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
            { MeasurementsGoldConstants.TransactionCreationDatetimeColumnName, ("TIMESTAMP", false) },
            { MeasurementsGoldConstants.IsCancelledColumnName, ("BOOLEAN", true) },
        };

    private static List<IEnumerable<string>> CreateRows()
    {
        var dates = new[]
        {
            (new LocalDate(2022, 1, 2), new LocalDate(2022, 1, 3), "measured", false),
            (new LocalDate(2022, 1, 2), new LocalDate(2022, 1, 4), "calculated", true),
            (new LocalDate(2022, 1, 3), new LocalDate(2022, 1, 4), "measured", false),
            (new LocalDate(2022, 1, 3), new LocalDate(2022, 1, 5), "measured", false),
            (new LocalDate(2022, 2, 1), new LocalDate(2022, 2, 2), "invalidQuality", false),
        };

        return [.. dates.SelectMany(CreateRow)];
    }

    private static IEnumerable<IEnumerable<string>> CreateRow((LocalDate ObservationTime, LocalDate TransactionCreationDate, string Quality, bool IsCancelled) values)
    {
        var observationDate = values.ObservationTime;
        var transactionCreationDate = values.TransactionCreationDate;
        var observationDateTime = Instant.FromUtc(observationDate.Year, observationDate.Month, observationDate.Day, 0, 0, 0);
        var transactionCreationDateTime = Instant.FromUtc(transactionCreationDate.Year, transactionCreationDate.Month, transactionCreationDate.Day, 0, 0, 0);

        return Enumerable.Range(0, 24).Select(i => new[]
        {
            "'1234567890'",
            "'kwh'",
            $"'{FormatString(observationDateTime.Plus(Duration.FromHours(i)))}'",
            $"{i}.4",
            $"'{values.Quality}'",
            $"'{FormatString(transactionCreationDateTime)}'",
            values.IsCancelled ? "true" : "false",
        });
    }

    private static string FormatString(Instant date)
    {
        return date.ToString("yyyy-MM-ddTHH:mm:ss'Z'", CultureInfo.InvariantCulture);
    }
}

using Energinet.DataHub.Core.FunctionApp.TestCommon.Configuration;
using Energinet.DataHub.Core.FunctionApp.TestCommon.Databricks;
using Energinet.DataHub.Measurements.WebApi;
using Microsoft.AspNetCore.Mvc.Testing;

namespace Energinet.DataHub.Measurements.Client.Tests.Fixtures;

public class MeasurementsClientAppFixture : WebApplicationFactory<Program>, IAsyncLifetime
{
    public DatabricksSchemaManager DatabricksSchemaManager { get; set; }

    public IntegrationTestConfiguration IntegrationTestConfiguration { get; }

    public MeasurementsClientAppFixture()
    {
        var httpClientFactory = new HttpClientFactory();
        IntegrationTestConfiguration = new IntegrationTestConfiguration();
        DatabricksSchemaManager = new DatabricksSchemaManager(
            new HttpClientFactory(),
            IntegrationTestConfiguration.DatabricksSettings,
            "mmcore_measurementsapi");
    }

    public Task InitializeAsync()
    {
        throw new NotImplementedException();
    }

    public Task DisposeAsync()
    {
        throw new NotImplementedException();
    }
}

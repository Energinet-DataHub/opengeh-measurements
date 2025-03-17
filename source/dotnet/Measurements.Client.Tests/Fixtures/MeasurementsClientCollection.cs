namespace Energinet.DataHub.Measurements.Client.Tests.Fixtures;

/// <summary>
/// A xUnit collection fixture for ensuring tests don't run in parallel.
///
/// xUnit documentation of collection fixtures:
///  * https://xunit.net/docs/shared-context#collection-fixture
/// </summary>
[CollectionDefinition(nameof(MeasurementsClientCollection))]
public class MeasurementsClientCollection : ICollectionFixture<MeasurementsClientFixture>
{
}

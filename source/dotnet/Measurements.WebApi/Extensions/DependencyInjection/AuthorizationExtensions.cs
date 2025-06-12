using Energinet.DataHub.MarketParticipant.Authorization.Extensions;
using Energinet.DataHub.Measurements.WebApi.Constants;
using Energinet.DataHub.RevisionLog.Integration;
using NodaTime;

namespace Energinet.DataHub.Measurements.WebApi.Extensions.DependencyInjection;

public static class AuthorizationExtensions
{
    public static IServiceCollection AddAuthorizationForWebApp(this IServiceCollection services)
    {
        services
            .AddAuthorizationVerifyModule()
            .AddEndpointAuthorizationModule(serviceProvider =>
            {
                var revisionLogClient = serviceProvider.GetRequiredService<IRevisionLogClient>();
                var clock = serviceProvider.GetRequiredService<IClock>();
                return log => revisionLogClient.LogAsync(new RevisionLogEntry(
                    Guid.NewGuid(),
                    MeasurementsSubsystem.Id,
                    log.Activity,
                    clock.GetCurrentInstant(),
                    log.Endpoint,
                    log.RequestId.ToString(),
                    affectedEntityType: log.EntityType,
                    affectedEntityKey: log.EntityKey));
            });
        return services;
    }
}

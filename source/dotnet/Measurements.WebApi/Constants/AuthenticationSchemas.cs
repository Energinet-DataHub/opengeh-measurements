using Microsoft.AspNetCore.Authentication.JwtBearer;

namespace Energinet.DataHub.Measurements.WebApi.Constants;

public static class AuthenticationSchemas
{
    // TODO: Remove this
    public const string Default = JwtBearerDefaults.AuthenticationScheme;

    public const string B2C = "B2C";
}

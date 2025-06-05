namespace Energinet.DataHub.Measurements.WebApi.Extensions;

public static class ObjectExtensions
{
    public static string ToSanitizedString<T>(this T input)
    {
        return Sanitized(input?.ToString());
    }

    private static string Sanitized(string? input)
    {
        return string.IsNullOrWhiteSpace(input) ?
            string.Empty :
            input.Replace("\n", string.Empty).Replace("\r", string.Empty).Trim();
    }
}

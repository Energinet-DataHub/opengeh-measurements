namespace Energinet.DataHub.Measurements.WebApi.Extensions;

public static class StringExtensions
{
    public static string Sanitize(this string? input)
    {
        return string.IsNullOrWhiteSpace(input) ?
            string.Empty :
            input.Replace("\n", string.Empty).Replace("\r", string.Empty);
    }

    public static string Sanitize<T>(this T input)
    {
        return input is null ? throw new ArgumentNullException(nameof(input)) : input.ToString().Sanitize();
    }
}

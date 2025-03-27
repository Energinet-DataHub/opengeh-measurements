using System.Reflection;

namespace Energinet.DataHub.Measurements.Client.Tests.Assets;

public static class TestAssets
{
    public static string MeasurementsForDayExcludingHistory => GetJsonFile($"{nameof(MeasurementsForDayExcludingHistory)}.json");

    public static string MeasurementsForMultipleDays => GetJsonFile($"{nameof(MeasurementsForMultipleDays)}.json");

    private static string GetJsonFile(string filename)
    {
        var assembly = Assembly.GetExecutingAssembly();
        var fullResourceName = $"{assembly.GetName().Name}.Assets.{filename}";

        using var stream = assembly.GetManifestResourceStream(fullResourceName)
                           ?? throw new FileNotFoundException($"Could not find embedded resource: {filename}");

        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }
}

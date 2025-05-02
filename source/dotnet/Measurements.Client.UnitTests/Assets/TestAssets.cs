using System.Reflection;

namespace Energinet.DataHub.Measurements.Client.UnitTests.Assets;

public static class TestAssets
{
    public static string MeasurementsForSingleDay => GetJsonFile($"{nameof(MeasurementsForSingleDay)}.json");

    public static string MeasurementsForDayWithHistoricalObservations => GetJsonFile($"{nameof(MeasurementsForDayWithHistoricalObservations)}.json");

    public static string MeasurementsAggregatedByDate => GetJsonFile($"{nameof(MeasurementsAggregatedByDate)}.json");

    public static string MeasurementsAggregatedByDateMissingMeasurements => GetJsonFile($"{nameof(MeasurementsAggregatedByDateMissingMeasurements)}.json");

    public static string MeasurementsAggregatedByMonth => GetJsonFile($"{nameof(MeasurementsAggregatedByMonth)}.json");

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

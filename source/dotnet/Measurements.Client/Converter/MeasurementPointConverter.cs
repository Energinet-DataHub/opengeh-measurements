using System.Text.Json;
using System.Text.Json.Serialization;
using Energinet.DataHub.Measurements.Abstractions.Api.Models;

namespace Energinet.DataHub.Measurements.Client.Converter;

public class MeasurementPointConverter : JsonConverter<IEnumerable<MeasurementPoint>>
{
    public override IEnumerable<MeasurementPoint>? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        using var document = JsonDocument.ParseValue(ref reader);

        var pointsElement = document.RootElement
            .GetProperty("Points")
            .EnumerateArray();

        var result = new List<MeasurementPoint>();
        var pointElements = pointsElement
            .Select(pointElement => pointElement.Deserialize<MeasurementPoint>(options));

        foreach (var point in pointElements)
        {
            if (point is null) throw new InvalidOperationException();
            result.Add(point);
        }

        return result.AsEnumerable();
    }

    public override void Write(Utf8JsonWriter writer, IEnumerable<MeasurementPoint> value, JsonSerializerOptions options)
    {
        writer.WriteStartObject();
        writer.WriteStartArray("Points");

        foreach (var point in value) JsonSerializer.Serialize(writer, point, options);

        writer.WriteEndArray();
        writer.WriteEndObject();
    }
}

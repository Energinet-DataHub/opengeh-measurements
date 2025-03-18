using System.Dynamic;
using Energinet.DataHub.Measurements.Application.Persistence;
using Energinet.DataHub.Measurements.Application.Responses;
using Energinet.DataHub.Measurements.Domain;
using Xunit;

namespace Energinet.DataHub.Measurements.UnitTests.Responses;

public class GetMeasurementResponseTests
{
    [Fact]
    public void Create_WhenMeasurementsExist_ThenReturnsGetMeasurementResponse()
    {
        // Arrange
        var measurements = new List<MeasurementsResult>
        {
            new(CreateRaw()),
            new(CreateRaw()),
            new(CreateRaw()),
        };

        // Act
        var actual = GetMeasurementResponse.Create(measurements);

        // Assert
        Assert.Equal("123456789", actual.MeteringPointId);
        Assert.Equal(Unit.kWh, actual.Unit);
        Assert.Equal(3, actual.Points.Count);
        Assert.True(actual.Points.All(p => p.Quantity == 42));
        Assert.True(actual.Points.All(p => p.Quality == Quality.Measured));
    }

    [Theory]
    [InlineData("KWH", Unit.kWh)]
    [InlineData("KW", Unit.kW)]
    [InlineData("MW", Unit.MW)]
    [InlineData("MWH", Unit.MWh)]
    [InlineData("TONNE", Unit.Tonne)]
    [InlineData("KVARH", Unit.kVArh)]
    [InlineData("MVAR", Unit.MVAr)]
    public void Create_WhenUnitKnown_ThenReturnsGetMeasurementResponse(string unit, Unit expectedUnit)
    {
        // Arrange
        var measurements = new List<MeasurementsResult>
        {
            new(CreateRaw(unit: unit)),
        };

        // Act
        var actual = GetMeasurementResponse.Create(measurements);

        // Assert
        Assert.Equal(expectedUnit, actual.Unit);
    }

    [Theory]
    [InlineData("measured", Quality.Measured)]
    [InlineData("estimated", Quality.Estimated)]
    [InlineData("calculated", Quality.Calculated)]
    [InlineData("missing", Quality.Missing)]
    public void Create_WhenQualityKnown_ThenReturnsGetMeasurementResponse(string quality, Quality expectedQuality)
    {
        // Arrange
        var measurements = new List<MeasurementsResult>
        {
            new(CreateRaw(quality: quality)),
        };

        // Act
        var actual = GetMeasurementResponse.Create(measurements);

        // Assert
        Assert.Equal(expectedQuality, actual.Points.First().Quality);
    }

    [Fact]
    public void Create_WhenQualityUnknown_ThenThrowsException()
    {
        // Arrange
        var measurements = new List<MeasurementsResult>
        {
            new(CreateRaw(quality: "UnknownQuality")),
        };

        // Act
        // Assert
        Assert.Throws<ArgumentOutOfRangeException>(() => GetMeasurementResponse.Create(measurements));
    }

    private static ExpandoObject CreateRaw(string unit = "kwh", string quality = "measured")
    {
        dynamic raw = new ExpandoObject();
        raw.metering_point_id = "123456789";
        raw.unit = unit;
        raw.observation_time = DateTimeOffset.Now;
        raw.quantity = 42;
        raw.quality = quality;
        return raw;
    }
}

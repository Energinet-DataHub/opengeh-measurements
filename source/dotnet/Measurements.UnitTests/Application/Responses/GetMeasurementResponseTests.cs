using System.Dynamic;
using Energinet.DataHub.Measurements.Application.Persistence;
using Energinet.DataHub.Measurements.Application.Responses;
using Energinet.DataHub.Measurements.Domain;
using NodaTime;
using Xunit;
using Xunit.Categories;

namespace Energinet.DataHub.Measurements.UnitTests.Application.Responses;

[UnitTest]
public class GetMeasurementResponseTests
{
    [Fact]
    public void Create_WhenMeasurementsExist_ThenReturnsGetMeasurementResponse()
    {
        // Arrange
        var date = DateTimeOffset.Now;
        var measurements = new List<MeasurementResult>
        {
            new(CreateRaw(date)),
            new(CreateRaw(date)),
            new(CreateRaw(date)),
        };

        // Act
        var actual = GetMeasurementResponse.Create(measurements);

        // Assert
        Assert.Equal(3, actual.Points.Count);
        foreach (var point in actual.Points)
        {
            Assert.Equal(Instant.FromDateTimeOffset(date), point.ObservationTime);
            Assert.Equal(42, point.Quantity);
            Assert.Equal(Unit.kWh, point.Unit);
            Assert.Equal(Quality.Measured, point.Quality);
            Assert.Equal(Instant.FromDateTimeOffset(date), point.Created);
            Assert.Equal(Instant.FromDateTimeOffset(date), point.TransactionCreated);
        }
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
        var measurements = new List<MeasurementResult>
        {
            new(CreateRaw(DateTimeOffset.Now, unit: unit)),
        };

        // Act
        var actual = GetMeasurementResponse.Create(measurements);

        // Assert
        Assert.Equal(expectedUnit, actual.Points.Single().Unit);
    }

    [Fact]
    public void Create_WhenUnitUnknown_ThenThrowsException()
    {
        // Arrange
        var measurements = new List<MeasurementResult>
        {
            new(CreateRaw(DateTimeOffset.Now, unit: "unknown")),
        };

        // Act
        // Assert
        Assert.Throws<ArgumentOutOfRangeException>(() => GetMeasurementResponse.Create(measurements));
    }

    [Theory]
    [InlineData("measured", Quality.Measured)]
    [InlineData("estimated", Quality.Estimated)]
    [InlineData("calculated", Quality.Calculated)]
    [InlineData("missing", Quality.Missing)]
    public void Create_WhenQualityKnown_ThenReturnsGetMeasurementResponse(string quality, Quality expectedQuality)
    {
        // Arrange
        var measurements = new List<MeasurementResult>
        {
            new(CreateRaw(DateTimeOffset.Now, quality: quality)),
        };

        // Act
        var actual = GetMeasurementResponse.Create(measurements);

        // Assert
        Assert.Equal(expectedQuality, actual.Points.Single().Quality);
    }

    [Fact]
    public void Create_WhenQualityUnknown_ThenThrowsException()
    {
        // Arrange
        var measurements = new List<MeasurementResult>
        {
            new(CreateRaw(DateTimeOffset.Now, quality: "unknown")),
        };

        // Act
        // Assert
        Assert.Throws<ArgumentOutOfRangeException>(() => GetMeasurementResponse.Create(measurements));
    }

    [Theory]
    [InlineData("PT15M", Resolution.QuarterHourly)]
    [InlineData("PT1H", Resolution.Hourly)]
    [InlineData("P1D", Resolution.Daily)]
    [InlineData("P1M", Resolution.Monthly)]
    [InlineData("P1Y", Resolution.Yearly)]
    public void Create_WhenResolutionKnown_ThenReturnsGetMeasurementResponse(string resolution, Resolution expectedResolution)
    {
        // Arrange
        var measurements = new List<MeasurementResult>
        {
            new(CreateRaw(DateTimeOffset.Now, resolution: resolution)),
        };

        // Act
        var actual = GetMeasurementResponse.Create(measurements);

        // Assert
        Assert.Equal(expectedResolution, actual.Points.Single().Resolution);
    }

    [Fact]
    public void Create_WhenResolutionUnknown_ThenThrowsException()
    {
        // Arrange
        var measurements = new List<MeasurementResult>
        {
            new(CreateRaw(DateTimeOffset.Now, resolution: "unknown")),
        };

        // Act
        // Assert
        Assert.Throws<ArgumentOutOfRangeException>(() => GetMeasurementResponse.Create(measurements));
    }

    private static ExpandoObject CreateRaw(DateTimeOffset date, string resolution = "PT1H", string unit = "kwh", string quality = "measured")
    {
        dynamic raw = new ExpandoObject();
        raw.unit = unit;
        raw.observation_time = date;
        raw.quantity = 42;
        raw.quality = quality;
        raw.resolution = resolution;
        raw.created = date;
        raw.transaction_creation_datetime = date;
        return raw;
    }
}

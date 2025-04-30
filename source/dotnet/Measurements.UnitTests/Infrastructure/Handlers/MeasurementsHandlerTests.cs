using System.Dynamic;
using AutoFixture.Xunit2;
using Energinet.DataHub.Measurements.Application.Exceptions;
using Energinet.DataHub.Measurements.Application.Persistence;
using Energinet.DataHub.Measurements.Application.Requests;
using Energinet.DataHub.Measurements.Domain;
using Energinet.DataHub.Measurements.Infrastructure.Extensions;
using Energinet.DataHub.Measurements.Infrastructure.Handlers;
using Moq;
using NodaTime;
using NodaTime.Extensions;
using Xunit;
using Xunit.Categories;

namespace Energinet.DataHub.Measurements.UnitTests.Infrastructure.Handlers;

[UnitTest]
public class MeasurementsHandlerTests
{
    [Theory]
    [InlineAutoData]
    public async Task GetByPeriod_WhenMeasurementsExist_ThenReturnsMeasurementsForPeriod(
        Mock<IMeasurementsRepository> measurementRepositoryMock)
    {
        // Arrange
        var date = new DateTimeOffset(2021, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var request = new GetByPeriodRequest("123456789", date, date.AddDays(1));
        var raw = CreateMeasurementsRaw(date);
        var measurementResult = new MeasurementResult(raw);
        measurementRepositoryMock
            .Setup(x => x.GetByPeriodAsync(It.IsAny<string>(), It.IsAny<Instant>(), It.IsAny<Instant>()))
            .Returns(AsyncEnumerable.Repeat(measurementResult, 1));
        var sut = new MeasurementsHandler(measurementRepositoryMock.Object);

        // Act
        var actual = await sut.GetByPeriodAsync(request);
        var actualPoint = actual.Points.Single();

        // Assert
        Assert.Equal(date, actualPoint.ObservationTime.ToDateTimeOffset());
        Assert.Equal(42, actualPoint.Quantity);
        Assert.Equal(Quality.Measured, actualPoint.Quality);
        Assert.Equal(Unit.kWh, actualPoint.Unit);
        Assert.Equal(date, actualPoint.Created.ToDateTimeOffset());
    }

    [Fact]
    public async Task GetByPeriodAsync_WhenMeasurementsNotExist_ThenThrowsNotFoundException()
    {
        // Arrange
        var date = new DateTimeOffset(2021, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var request = new GetByPeriodRequest("123456789", date, date.AddDays(1));
        var measurementRepositoryMock = new Mock<IMeasurementsRepository>();
        measurementRepositoryMock
            .Setup(x => x.GetByPeriodAsync(It.IsAny<string>(), It.IsAny<Instant>(), It.IsAny<Instant>()))
            .Returns(AsyncEnumerable.Empty<MeasurementResult>());
        var sut = new MeasurementsHandler(measurementRepositoryMock.Object);

        // Act
        // Assert
        await Assert.ThrowsAsync<MeasurementsNotFoundDuringPeriodException>(() => sut.GetByPeriodAsync(request));
    }

    [Theory]
    [InlineAutoData]
    public async Task GetAggregatedByDateAsync_WhenMeasurementsExist_ThenReturnsMeasurementsForPeriod(
        Mock<IMeasurementsRepository> measurementRepositoryMock)
    {
        // Arrange
        var yearMonth = new YearMonth(2021, 1);
        var request = new GetAggregatedByDateRequest("123456789", yearMonth.Year, yearMonth.Month);
        var raw = CreateAggregatedMeasurementsRaw(yearMonth);
        var measurementResult = new AggregatedMeasurementsResult(raw);
        measurementRepositoryMock
            .Setup(x => x.GetAggregatedByDateAsync(It.IsAny<string>(), It.IsAny<YearMonth>()))
            .Returns(AsyncEnumerable.Repeat(measurementResult, 1));
        var sut = new MeasurementsHandler(measurementRepositoryMock.Object);

        // Act
        var actual = await sut.GetAggregatedByDateAsync(request);
        var actualAggregations = actual.MeasurementAggregations.Single();

        // Assert
        Assert.Equal(yearMonth.ToDateInterval().Start, actualAggregations.Date.ToLocalDate());
        Assert.Equal(42, actualAggregations.Quantity);
        Assert.Equal(Quality.Measured, actualAggregations.Quality);
        Assert.Equal(Unit.kWh, actualAggregations.Unit);
        Assert.True(actualAggregations.MissingValues);
        Assert.True(actualAggregations.ContainsUpdatedValues);
    }

    [Fact]
    public async Task GetAggregatedByDateAsync_WhenMeasurementsNotExist_ThenThrowsNotFoundException()
    {
        // Arrange
        var request = new GetAggregatedByDateRequest("123456789", 2021, 1);
        var measurementRepositoryMock = new Mock<IMeasurementsRepository>();
        measurementRepositoryMock
            .Setup(x => x.GetAggregatedByDateAsync(It.IsAny<string>(), It.IsAny<YearMonth>()))
            .Returns(AsyncEnumerable.Empty<AggregatedMeasurementsResult>());
        var sut = new MeasurementsHandler(measurementRepositoryMock.Object);

        // Act
        // Assert
        await Assert.ThrowsAsync<MeasurementsNotFoundDuringPeriodException>(() => sut.GetAggregatedByDateAsync(request));
    }

    [Theory]
    [InlineAutoData]
    public async Task GetAggregatedByMonthAsync_WhenMeasurementsExist_ThenReturnsMeasurementsForPeriod(
        Mock<IMeasurementsRepository> measurementRepositoryMock)
    {
        // Arrange
        var yearMonth = new YearMonth(2021, 1);
        var request = new GetAggregatedByMonthRequest("123456789", yearMonth.Year);
        var raw = CreateAggregatedMeasurementsRaw(yearMonth);
        var measurementResult = new AggregatedMeasurementsResult(raw);
        measurementRepositoryMock
            .Setup(x => x.GetAggregatedByMonthAsync(It.IsAny<string>(), It.IsAny<Year>()))
            .Returns(AsyncEnumerable.Repeat(measurementResult, 1));
        var sut = new MeasurementsHandler(measurementRepositoryMock.Object);

        // Act
        var actual = await sut.GetAggregatedByMonthAsync(request);
        var actualAggregations = actual.MeasurementAggregations.Single();

        // Assert
        Assert.Equal(yearMonth.ToDateInterval().Start, actualAggregations.YearMonth.ToDateInterval().Start);
        Assert.Equal(yearMonth.ToDateInterval().End, actualAggregations.YearMonth.ToDateInterval().End);
        Assert.Equal(42, actualAggregations.Quantity);
        Assert.Equal(Quality.Measured, actualAggregations.Quality);
        Assert.Equal(Unit.kWh, actualAggregations.Unit);
    }

    [Fact]
    public async Task GetAggregatedByMonthAsync_WhenMeasurementsNotExist_ThenThrowsNotFoundException()
    {
        // Arrange
        var request = new GetAggregatedByMonthRequest("123456789", 2021);
        var measurementRepositoryMock = new Mock<IMeasurementsRepository>();
        measurementRepositoryMock
            .Setup(x => x.GetAggregatedByMonthAsync(It.IsAny<string>(), It.IsAny<Year>()))
            .Returns(AsyncEnumerable.Empty<AggregatedMeasurementsResult>());
        var sut = new MeasurementsHandler(measurementRepositoryMock.Object);

        // Act
        // Assert
        await Assert.ThrowsAsync<MeasurementsNotFoundDuringPeriodException>(() => sut.GetAggregatedByMonthAsync(request));
    }

    private static dynamic CreateMeasurementsRaw(DateTimeOffset now)
    {
        dynamic raw = new ExpandoObject();
        raw.unit = "kwh";
        raw.observation_time = now;
        raw.quantity = 42;
        raw.quality = "measured";
        raw.resolution = "PT1H";
        raw.created = now;
        raw.transaction_creation_datetime = now;
        return raw;
    }

    private static dynamic CreateAggregatedMeasurementsRaw(YearMonth yearMonth)
    {
        var (minObservationTime, maxObservationTime) = yearMonth.ToDateInterval();

        dynamic raw = new ExpandoObject();
        raw.min_observation_time = minObservationTime.ToDateTimeOffSet();
        raw.max_observation_time = maxObservationTime.ToDateTimeOffSet();
        raw.aggregated_quantity = 42;
        raw.qualities = new[] { "measured" };
        raw.resolutions = new[] { "PT1H" };
        raw.units = new[] { "kWh" };
        raw.point_count = 1;
        raw.observation_updates = 2;
        return raw;
    }
}

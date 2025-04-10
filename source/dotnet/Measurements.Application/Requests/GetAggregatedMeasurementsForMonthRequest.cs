using System.ComponentModel.DataAnnotations;

namespace Energinet.DataHub.Measurements.Application.Requests;

public record GetAggregatedMeasurementsForMonthRequest(string MeteringPointId, [Range(-9998, 9999)] int Year, [Range(1, 12)] int Month);

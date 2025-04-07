using System.ComponentModel.DataAnnotations;

namespace Energinet.DataHub.Measurements.Application.Requests;

public record GetAggregatedMeasurementsForMonthRequest(
    string MeteringPointId,
    [Range(-9998, 9999, ErrorMessage = "{0} must be between {1} and {2}")] int Year,
    [Range(1, 12, ErrorMessage = "{0} must be between {1} and {2}")] int Month);

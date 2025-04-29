using System.ComponentModel.DataAnnotations;
using NodaTime;

namespace Energinet.DataHub.Measurements.Domain;

public readonly struct Year([Range(-9998, 9999)] int year)
{
    public int GetYear() => year;

    private LocalDate StartDate { get; } = new(year, 1, 1);

    private LocalDate EndDate { get; } = new(year, 12, 31);

    public (LocalDate StartDate, LocalDate EndDate) ToDateInterval()
    {
        return (StartDate, EndDate);
    }
}

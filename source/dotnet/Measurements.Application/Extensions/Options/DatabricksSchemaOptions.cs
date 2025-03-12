using System.ComponentModel.DataAnnotations;

namespace Energinet.DataHub.Measurements.Application.Extensions.Options;

public class DatabricksSchemaOptions
{
    [Required]
    public string SchemaName { get; init; } = null!;

    [Required]
    public string CatalogName { get; init; } = null!;
}

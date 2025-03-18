using System.ComponentModel.DataAnnotations;

namespace Energinet.DataHub.Measurements.Application.Extensions.Options;

public class DatabricksSchemaOptions
{
    public const string SectionName = "DatabricksSchemaOptions";

    [Required(AllowEmptyStrings = false)]
    public string SchemaName { get; init; } = string.Empty;

    [Required(AllowEmptyStrings = false)]
    public string CatalogName { get; init; } = string.Empty;
}

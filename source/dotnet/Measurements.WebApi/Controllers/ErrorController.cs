using System.Web;
using Asp.Versioning;
using Energinet.DataHub.Measurements.WebApi.Extensions;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Diagnostics;
using Microsoft.AspNetCore.Mvc;

namespace Energinet.DataHub.Measurements.WebApi.Controllers;

[ApiController]
[AllowAnonymous]
[ApiVersionNeutral]
public class ErrorController(ILogger<ErrorController> logger) : ControllerBase
{
    [ApiExplorerSettings(IgnoreApi = true)]
    [Route("/error")]
    public IActionResult HandleError()
    {
        var exception = HttpContext.Features.Get<IExceptionHandlerFeature>()
                        ?? throw new InvalidOperationException("Exception feature is null");
        var queryString = HttpUtility.HtmlEncode(Request.QueryString);
        var requestPath = HttpContext.Request.Path.Value ?? throw new InvalidOperationException("Request path is null");

        logger.LogError(
            exception.Error,
            "An unknown error has occured.\nEndpoint path: {},\nRequest: {}",
            exception.Path.Sanitize(),
            queryString);

        return Problem(
            detail: "An unknown error occured while handling request to the Measurements API. Try again later.",
            instance: requestPath,
            statusCode: StatusCodes.Status500InternalServerError);
    }
}

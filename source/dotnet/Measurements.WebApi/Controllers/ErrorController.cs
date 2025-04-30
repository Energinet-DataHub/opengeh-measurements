using System.Diagnostics;
using System.Web;
using Asp.Versioning;
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
            "An unknown error has occured. \n Endpoint path: {}, \n Request: {}",
            SanitizeRequestPath(exception.Path),
            queryString);

        return Problem(
            detail: "An unknown error occured while handling request to the Measurements API. Try again later.",
            instance: requestPath,
            statusCode: StatusCodes.Status500InternalServerError);
    }

    private static string SanitizeRequestPath(string requestPath)
    {
        return requestPath.Replace("\n", string.Empty).Replace("\r", string.Empty);
    }
}

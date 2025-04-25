using System.Web;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Diagnostics;
using Microsoft.AspNetCore.Mvc;

namespace Energinet.DataHub.Measurements.WebApi.Controllers;

[ApiController]
[Authorize]
public class ErrorController(ILogger<ErrorController> logger) : ControllerBase
{
    [ApiExplorerSettings(IgnoreApi = true)]
    [Route("/error")]
    public IActionResult HandleError()
    {
        var exception = HttpContext.Features.Get<IExceptionHandlerFeature>();
        var queryString = HttpUtility.HtmlEncode(Request.QueryString);

        if (exception != null)
        {
            logger.LogError(
                exception.Error,
                "An unknown error has occured. \n Endpoint path: {}, \n Request: {}",
                SanitizeRequestPath(exception.Path),
                queryString);

            return Problem(
                detail: exception.Error.Message,
                instance: exception.Path,
                statusCode: StatusCodes.Status500InternalServerError);
        }

        logger.LogError(
            "An unknown error has occured. \n Endpoint path: {}, \n Request: {}",
            SanitizeRequestPath(Request.Path),
            queryString);

        var requestPath = HttpContext.Request.Path.Value ?? throw new InvalidOperationException("Request path is null");

        return Problem(
            detail: "An unknown error occured.",
            instance: SanitizeRequestPath(requestPath),
            statusCode: StatusCodes.Status500InternalServerError);
    }

    private static string SanitizeRequestPath(string requestPath)
    {
        return requestPath.Replace("\n", string.Empty).Replace("\r", string.Empty);
    }
}

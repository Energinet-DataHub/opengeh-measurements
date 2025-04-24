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
                exception.Path,
                queryString);

            return Problem(
                detail: exception.Error.Message,
                instance: exception.Path,
                statusCode: StatusCodes.Status500InternalServerError);
        }

        logger.LogError(
            "An unknown error has occured. \n Endpoint path: {}, \n Request: {}",
            HttpContext.Request.Path,
            queryString);

        return Problem(
            detail: "An unknown error occured.",
            instance: HttpContext.Request.Path,
            statusCode: StatusCodes.Status500InternalServerError);
    }
}

namespace MMD.TenantManagement.HealthCheck;

/// <summary>
/// Health controller used by web test to check availability
/// </summary>
[ApiController]
[ExceptionFilter]
[Route("v{version:apiVersion}/health")]
[Produces("application/json")]
public sealed class HealthCheckController : TenantManagementControllerBase
{
    private const string ThisController = nameof(HealthCheckController);
    private const string GetRoute = ThisController + "GetHealthCheck";

    private static readonly HashSet<string> SecureAgents = new()
    {
        "Azure Application Insights",
        "Azure Traffic Manager Endpoint Monitor",
        "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0; AppInsights)",
        "HealthCheck/1.0",
        "ReadyForRequest/1.0 (HealthCheck)",
        "VSTS_cb55739e-4afe-46a3-970f-1b49d8ee7564_Gates_ServerExecution_HttpRequest"
    };

    private readonly ITelemetryService telemetryService;
    private readonly IAvailabilityService availabilityService;

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="telemetryService">Telemetry service</param>
    /// <param name="availabilityService">Health service</param>
    public HealthCheckController(
       ITelemetryService telemetryService,
       IAvailabilityService availabilityService)
    {
        this.telemetryService = telemetryService;
        this.availabilityService = availabilityService;
    }

    /// <summary>
    ///     Check health endpoint
    /// </summary>
    /// <returns>Empty response</returns>
    [HttpGet("")]
    [AllowAnonymous]
    [ProducesResponseType(Status200OK)]
    [ProducesResponseType(Status401Unauthorized)]
    [ProducesResponseType(Status503ServiceUnavailable)]
    [SkipTenantValidation("Not related to specific tenant")]
    public async Task<ActionResult> Get()
    {
        var properties = GetTelemetryProperties();
        properties.Add("Load", "HealthCheck");

        using var telemetryOperation = telemetryService.CreateTelemetryOperation($"/health", properties);

        var userAgents = Request.Headers.ContainsKey("User-Agent") ? Request.Headers["User-Agent"] : Request.Headers["X-Ms-User-Agent"];

        if (userAgents.Count is 0)
        {
            return Unauthorized();
        }

        var userAgent = userAgents[0];
        telemetryOperation.AddProperty("UserAgent", userAgent);

        if (!SecureAgents.Contains(userAgent))
        {
            return Unauthorized();
        }

        var isServiceAvailable = await availabilityService.CheckDataConnectivityAsync();

        if (isServiceAvailable)
        {
            return Ok();
        }
        else
        {
            return StatusCode(Status503ServiceUnavailable);
        }
    }
}

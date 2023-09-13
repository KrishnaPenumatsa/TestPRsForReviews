// ----------------------------------------------------------------------
// <copyright file="OfficeUpdatesController.cs" company="Microsoft Corporation">
// Copyright © Microsoft Corporation. All rights reserved.
// </copyright>
// ----------------------------------------------------------------------

namespace MMD.UpdateManagement.Controllers.FirstParty;

/// <summary>
/// Office updates controller
/// </summary>
[ApiController]
[Route("v{version:apiVersion}/[controller]")]
[Authorize(AuthenticationSchemes = MmdAuthenticationSchemes.CustomerFirstPartyAuthenticationScheme, Roles = UpdateManagementConstants.DEFAULT_CUSTOMER_MODIFIER_ROLES)]
[TenantValidation(TenantIdSource.AuthToken)]
public class OfficeUpdatesController : UpdateManagementControllerBase
{
    private const string FlightName = "EnableOfficeServiceProfile";

    private readonly ITelemetryService telemetryService;
    private readonly IOfficeApiGatewayFactory officeApiGatewayFactory;
    private readonly IFlightingResolver flightingResolver;

    /// <summary>
    /// Initializes a new instance of the <see cref="OfficeUpdatesController"/> class
    /// </summary>
    /// <param name="telemetryService">telemetry service</param>
    /// <param name="officeApiGatewayFactory">office api gateway factory</param>
    /// <param name="flightingResolver">flighting resolver</param>
    public OfficeUpdatesController(
        ITelemetryService telemetryService,
        IOfficeApiGatewayFactory officeApiGatewayFactory,
        IFlightingResolver flightingResolver)
    {
        this.telemetryService = telemetryService;
        this.officeApiGatewayFactory = officeApiGatewayFactory;
        this.flightingResolver = flightingResolver;
    }

    /// <summary>
    /// Gets a list of office service profile
    /// </summary>
    /// <returns>list of service profiles</returns>
    [HttpGet("serviceProfile")]
    [ProducesResponseType(Status200OK)]
    [ProducesResponseType(Status400BadRequest)]
    [ProducesResponseType(Status404NotFound)]
    [ProducesResponseType(Status405MethodNotAllowed)]
    [ProducesResponseType(Status500InternalServerError)]
    public async Task<ActionResult<List<ServiceProfile>>> GetOfficeServiceProfilesAsync()
    {
        using var telemetryOperation = telemetryService.CreateTelemetryOperation(
            nameof(GetOfficeServiceProfilesAsync));

        try
        {
            var tenantId = User.GetTenantId().Value;
            telemetryOperation.AddProperty("TenantId", tenantId);

            bool isFlightEnabled = await flightingResolver.IsFlightEnabled(FlightName, tenantId);

            if (isFlightEnabled)
            {
                IOfficeApiGateway officeApiGateway = officeApiGatewayFactory.CreateAuthenticatedOfficeApiGateway(User, tenantId);

                var response = await officeApiGateway.GetOfficeServiceProfilesAsync();
                return Ok(response);
            }
            else
            {
                telemetryOperation.TrackTrace($"Tenant {tenantId} is not allowed to access the office service profile feature.");
                return StatusCode(Status405MethodNotAllowed, $"Tenant {tenantId} is not allowed to access the office service profile feature.");
            }
        }
        catch (ArgumentException e)
        {
            return BadRequest(e.Message);
        }
        catch (HttpException e) when (e.StatusCode == HttpStatusCode.NotFound)
        {
            // Office service profile could be not created yet.
            return NotFound(e.Message);
        }
        catch (Exception e)
        {
            telemetryOperation.TrackException(e);
            return StatusCode(Status500InternalServerError, $"error msg: {e.Message}");
        }
    }

    /// <summary>
    /// Gets service profile rules
    /// </summary>
    /// <param name="serviceProfileId">service profile id</param>
    /// <returns>service profile rules</returns>
    [HttpGet("serviceProfile/{serviceProfileId}/serviceProfileRules")]
    [ProducesResponseType(Status200OK)]
    [ProducesResponseType(Status400BadRequest)]
    [ProducesResponseType(Status404NotFound)]
    [ProducesResponseType(Status405MethodNotAllowed)]
    [ProducesResponseType(Status500InternalServerError)]
    public async Task<ActionResult<ServiceProfileRules>> GetOfficeServiceProfileRulesAsync(Guid serviceProfileId)
    {
        using var telemetryOperation = telemetryService.CreateTelemetryOperation(
            nameof(GetOfficeServiceProfileRulesAsync));

        try
        {
            Guard.NotDefault(serviceProfileId, nameof(serviceProfileId));

            var tenantId = User.GetTenantId().Value;

            telemetryOperation.AddPropertiesFromDictionary(
                new()
                {
                    { "TenantId", tenantId },
                    { "ServiceProfileId", serviceProfileId }
                });

            bool isFlightEnabled = await flightingResolver.IsFlightEnabled(FlightName, tenantId);

            if (isFlightEnabled)
            {
                IOfficeApiGateway officeApiGateway = officeApiGatewayFactory.CreateAuthenticatedOfficeApiGateway(User, tenantId);

                var response = await officeApiGateway.GetOfficeServiceProfileRulesAsync(serviceProfileId);
                return Ok(response);
            }
            else
            {
                telemetryOperation.TrackTrace($"Tenant {tenantId} is not allowed to access the office service profile feature.");
                return StatusCode(Status405MethodNotAllowed, $"Tenant {tenantId} is not allowed to access the office service profile rules feature.");
            }
        }
        catch (ArgumentException e)
        {
            return BadRequest(e.Message);
        }
        catch (HttpException e) when (e.StatusCode == HttpStatusCode.NotFound)
        {
            // Office service profile rules could be not created yet.
            return NotFound(e.Message);
        }
        catch (Exception e)
        {
            telemetryOperation.TrackException(e);
            return StatusCode(Status500InternalServerError, $"error msg: {e.Message}");
        }
    }

    /// <summary>
    /// Upserts a service profile rules
    /// </summary>
    /// <param name="rules">rules object</param>
    /// <returns>ObjectResult</returns>
    [HttpPut("serviceProfile/serviceProfileRules")]
    [ProducesResponseType(Status204NoContent)]
    [ProducesResponseType(Status400BadRequest)]
    [ProducesResponseType(Status404NotFound)]
    [ProducesResponseType(Status405MethodNotAllowed)]
    [ProducesResponseType(Status500InternalServerError)]
    public async Task<ActionResult> UpsertOfficeServiceProfileRulesAsync(ServiceProfileRules rules)
    {
        using var telemetryOperation = telemetryService.CreateTelemetryOperation(
            nameof(UpsertOfficeServiceProfileRulesAsync));

        try
        {
            Guard.NotNull(rules, nameof(rules));
            Guard.NotDefault(rules.ProfileId, nameof(rules.ProfileId));

            var tenantId = User.GetTenantId().Value;

            telemetryOperation.AddPropertiesFromDictionary(
                new()
                {
                    { "TenantId", tenantId },
                    { "ProfileId", rules.ProfileId }
                });

            bool isFlightEnabled = await flightingResolver.IsFlightEnabled(FlightName, tenantId);

            if (isFlightEnabled)
            {
                IOfficeApiGateway officeApiGateway = officeApiGatewayFactory.CreateAuthenticatedOfficeApiGateway(User, tenantId);

                await officeApiGateway.UpsertOfficeServiceProfileRulesAsync(rules);
                return NoContent();
            }
            else
            {
                telemetryOperation.TrackTrace($"Tenant {tenantId} is not allowed to access the office service profile feature.");
                return StatusCode(Status405MethodNotAllowed, $"Tenant {tenantId} is not allowed to access the office service profile rules upsert feature.");
            }
        }
        catch (ArgumentException e)
        {
            return BadRequest(e.Message);
        }
        catch (HttpException e) when (e.StatusCode == HttpStatusCode.NotFound)
        {
            // Office service profile rules could be not created yet.
            return NotFound(e.Message);
        }
        catch (Exception e)
        {
            telemetryOperation.TrackException(e);
            return StatusCode(Status500InternalServerError, $"error msg: {e.Message}");
        }
    }

    /// <summary>
    /// Updates a service profile
    /// </summary>
    /// <param name="serviceProfile">service profile object</param>
    /// <returns>ObjectResult</returns>
    [HttpPut("serviceProfile")]
    [ProducesResponseType(Status204NoContent)]
    [ProducesResponseType(Status400BadRequest)]
    [ProducesResponseType(Status404NotFound)]
    [ProducesResponseType(Status405MethodNotAllowed)]
    [ProducesResponseType(Status500InternalServerError)]
    public async Task<ActionResult> UpdateOfficeServiceProfileAsync(ServiceProfile serviceProfile)
    {
        using var telemetryOperation = telemetryService.CreateTelemetryOperation(
            nameof(UpdateOfficeServiceProfileAsync));

        try
        {
            Guard.NotNull(serviceProfile, nameof(serviceProfile));
            Guard.NotDefault(serviceProfile.ServiceProfileId, nameof(serviceProfile.ServiceProfileId));

            var tenantId = User.GetTenantId().Value;

            telemetryOperation.AddPropertiesFromDictionary(
                new()
                {
                    { "TenantId", tenantId },
                    { "ServiceProfileId", serviceProfile.ServiceProfileId }
                });

            bool isFlightEnabled = await flightingResolver.IsFlightEnabled(FlightName, tenantId);

            if (isFlightEnabled)
            {
                IOfficeApiGateway officeApiGateway = officeApiGatewayFactory.CreateAuthenticatedOfficeApiGateway(User, tenantId);

                await officeApiGateway.UpdateOfficeServiceProfileAsync(serviceProfile);
                return NoContent();
            }
            else
            {
                telemetryOperation.TrackTrace($"Tenant {tenantId} is not allowed to access the office service profile feature.");
                return StatusCode(Status405MethodNotAllowed, $"Tenant {tenantId} is not allowed to access the office service profile rules upsert feature.");
            }
        }
        catch (ArgumentException e)
        {
            return BadRequest(e.Message);
        }
        catch (HttpException e) when (e.StatusCode == HttpStatusCode.NotFound)
        {
            // Office service profile could be not created yet.
            return NotFound(e.Message);
        }
        catch (Exception e)
        {
            telemetryOperation.TrackException(e);
            return StatusCode(Status500InternalServerError, $"error msg: {e.Message}");
        }
    }

    /// <summary>
    /// Provisions service profile dependencies
    /// </summary>
    /// <returns>ObjectResult</returns>
    [HttpPost("provisionServiceProfileDependencies")]
    [ProducesResponseType(Status204NoContent)]
    [ProducesResponseType(Status400BadRequest)]
    [ProducesResponseType(Status405MethodNotAllowed)]
    [ProducesResponseType(Status500InternalServerError)]
    public async Task<ActionResult> ProvisionOfficeServiceProfileDependenciesAsync()
    {
        using var telemetryOperation = telemetryService.CreateTelemetryOperation(
            nameof(ProvisionOfficeServiceProfileDependenciesAsync));

        try
        {
            var tenantId = User.GetTenantId().Value;

            telemetryOperation.AddPropertiesFromDictionary(
                new()
                {
                    { "TenantId", tenantId }
                });

            bool isFlightEnabled = await flightingResolver.IsFlightEnabled(FlightName, tenantId);

            if (isFlightEnabled)
            {
                IOfficeOnboardingApiGateway officeOnboardingApiGateway = officeApiGatewayFactory.CreateAuthenticatedOfficeOnboardingApiGateway(User, tenantId);

                // Accept consent
                await officeOnboardingApiGateway.AcceptConsentAsync(new Consent.ConsentValue()
                {
                    IsAccepted = true,
                    LearningConsent = "LearningConsentB",
                    Version = "v1"
                });

                // Provision dependency services
                await officeOnboardingApiGateway.ProvisionDependencyServicesAsync();

                return NoContent();
            }
            else
            {
                telemetryOperation.TrackTrace($"Tenant {tenantId} is not allowed to access the office service profile feature.");
                return StatusCode(Status405MethodNotAllowed, $"Tenant {tenantId} is not allowed to access the office service profile feature.");
            }
        }
        catch (ArgumentException e)
        {
            return BadRequest(e.Message);
        }
        catch (Exception e)
        {
            telemetryOperation.TrackException(e);
            return StatusCode(Status500InternalServerError, $"error msg: {e.Message}");
        }
    }

    /// <summary>
    /// Checks whether service profile dependencies are provisioned
    /// </summary>
    /// <returns>true of false</returns>
    [HttpGet("isServiceProfileDependenciesProvisioned")]
    [ProducesResponseType(Status200OK)]
    [ProducesResponseType(Status400BadRequest)]
    [ProducesResponseType(Status405MethodNotAllowed)]
    [ProducesResponseType(Status500InternalServerError)]
    public async Task<ActionResult<bool>> IsOfficeServiceProfileDependenciesProvisionedAsync()
    {
        using var telemetryOperation = telemetryService.CreateTelemetryOperation(
            nameof(IsOfficeServiceProfileDependenciesProvisionedAsync));

        try
        {
            var tenantId = User.GetTenantId().Value;

            telemetryOperation.AddPropertiesFromDictionary(
                new()
                {
                    { "TenantId", tenantId }
                });

            bool isFlightEnabled = await flightingResolver.IsFlightEnabled(FlightName, tenantId);

            if (isFlightEnabled)
            {
                IOfficeOnboardingApiGateway officeOnboardingApiGateway = officeApiGatewayFactory.CreateAuthenticatedOfficeOnboardingApiGateway(User, tenantId);

                var consent = await officeOnboardingApiGateway.GetConsentAsync();
                var featureProvisionData = await officeOnboardingApiGateway.GetFeatureProvisionDataAsync();

                var isLearningConsentBAccepted = consent?.Values.Find(x => x.LearningConsent == "LearningConsentB")?.IsAccepted == true;
                var isInventoryProvisioned = featureProvisionData?.Values.Find(x => x.FeatureName == "Inventory")?.FeatureProvisionStatus == "Provisioned";
                var isTenantAssociationKeyProvisioned = featureProvisionData?.Values.Find(x => x.FeatureName == "TenantAssociationKey")?.FeatureProvisionStatus == "Provisioned";

                return Ok(isLearningConsentBAccepted && isInventoryProvisioned && isTenantAssociationKeyProvisioned);
            }
            else
            {
                telemetryOperation.TrackTrace($"Tenant {tenantId} is not allowed to access the office service profile feature.");
                return StatusCode(Status405MethodNotAllowed, $"Tenant {tenantId} is not allowed to access the office service profile feature.");
            }
        }
        catch (ArgumentException e)
        {
            return BadRequest(e.Message);
        }
        catch (Exception e)
        {
            telemetryOperation.TrackException(e);
            return StatusCode(Status500InternalServerError, $"error msg: {e.Message}");
        }
    }
}
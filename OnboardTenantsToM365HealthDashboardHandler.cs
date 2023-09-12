using MMD.TenantManagement.Contracts.Monolith;
using MMD.TenantManagement.Functions.OperationsOrchestrator.Enrollment.Requests;
using MMD.TenantManagement.Services.Common.Configuration;

namespace MMD.TenantManagement.Functions.OperationsOrchestrator.Enrollment.Handlers;

public sealed class OnboardTenantsToM365HealthDashboardHandler : IRequestHandler<OnboardTenantsToM365HealthDashboardRequest>
{
    private readonly ITelemetryService telemetryService;
    private readonly ITenantGateway tenantGateway;
    private readonly IM365HealthDashboardRepository m365HealthDashboardRepository;
    private readonly IConfigurationResolver<TenantManagementConfiguration> serviceConfigurationResolver;

    public OnboardTenantsToM365HealthDashboardHandler(
        ITelemetryService telemetryService,
        ITenantGateway tenantGateway,
        IM365HealthDashboardRepository m365HealthDashboardRepository,
        IConfigurationResolver<TenantManagementConfiguration> serviceConfigurationResolver)
    {
        this.telemetryService = telemetryService;
        this.tenantGateway = tenantGateway;
        this.m365HealthDashboardRepository = m365HealthDashboardRepository;
        this.serviceConfigurationResolver = serviceConfigurationResolver;
    }

    public async Task HandleAsync(OnboardTenantsToM365HealthDashboardRequest message)
    {
        using var telemetryOperation = telemetryService.CreateTelemetryOperation(nameof(OnboardTenantsToM365HealthDashboard));
        try
        {
            var tenants = await tenantGateway.GetTenants(enrolledOnly: true, includeTombstoned: true, excludeReadOnly: true);

            await UpdateTopologyAndTenantsAsync(tenants, telemetryOperation, PlanType.Premium, PlanType.Standard);
            await UpdateTopologyAndTenantsAsync(tenants, telemetryOperation, PlanType.Starter);
        }
        catch (Exception ex)
        {
            telemetryOperation.TrackTrace($"Failed to execute {nameof(OnboardTenantsToM365HealthDashboard)} with message {ex.Message}");
            telemetryOperation.TrackException(ex);
            throw;
        }
    }

    private async Task UpdateTopologyAndTenantsAsync(IEnumerable<Tenant> tenants, ITelemetryOperation telemetryOperation, PlanType workloadPlanType, PlanType? altPlan = null)
    {
        var serviceConfiguration = serviceConfigurationResolver.GetConfig();
        try
        {
            var workloadTenants = tenants.Where(x => x.IsPartiallyEnrolledOrEnrolled(workloadPlanType) || x.State == TenantState.Tombstoned);
            if (altPlan.HasValue)
            {
                workloadTenants = workloadTenants.Union(tenants.Where(x => x.IsPartiallyEnrolledOrEnrolled(altPlan.Value)));
            }
            var tenantMessages = workloadTenants.Select(tenant => new OnboardTenantToM365EventAuthoringMessage
            {
                TenantId = tenant.DirectoryId,
                InfrastructureName = serviceConfiguration.M365InfrastructureName,
                IsDeleted = tenant.State == TenantState.Tombstoned
            }).ToArray();

            var topologyTxnId = await m365HealthDashboardRepository.UpdateTopologyAsync(workloadPlanType);
            telemetryOperation.TrackTrace($"{workloadPlanType} Topology Infrastructure updated, transactionid:{topologyTxnId}");

            var tenantUpdateTxnId = await m365HealthDashboardRepository.UpdateTenantsAsync(workloadPlanType, tenantMessages.ToArray());
            telemetryOperation.TrackTrace($"Onboarded {tenantMessages.Length} {workloadPlanType} tenants to M365HealthDashboard {serviceConfiguration.M365InfrastructureName} with transactionid {tenantUpdateTxnId}");
        }
        catch (Exception ex)
        {
            telemetryOperation.TrackTrace($"Failed to execute {nameof(UpdateTopologyAndTenantsAsync)} for plan {workloadPlanType} with message {ex.Message}");
            telemetryOperation.TrackException(ex);
            throw;
        }
    }
}
using Microsoft.Azure.Functions.Worker;
using MMD.TenantManagement.Functions.OperationsOrchestrator.Enrollment.Requests;

namespace MMD.TenantManagement.Functions.OperationsOrchestrator.Enrollment;

public sealed class OnboardTenantsToM365HealthDashboard
{
    private const string Schedule = "0 3 * * *"; // 0300 UTC => 11:00 AM China time, 8:00 PM PST time

    private readonly IMediator mediator;

    public OnboardTenantsToM365HealthDashboard(IMediator mediator) => this.mediator = mediator;

    [Function(nameof(OnboardTenantsToM365HealthDashboard))]
    [SuppressMessage("Style", "IDE0060:Remove unused parameter", Justification = "N/A")]
    public Task Run([TimerTrigger(Schedule)] TimerInfo timer)
    {
        return this.mediator.HandleAsync(new OnboardTenantsToM365HealthDashboardRequest());
    }
}
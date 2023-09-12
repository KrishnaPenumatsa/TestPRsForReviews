using Microsoft.Graph;
using MMD.Core;
using MMD.Core.Extensions;
using MMD.Core.Telemetry;
using MMD.TenantManagement.Contracts.Operations;
using MMD.TenantManagement.Services.Graph;
using MMD.TenantManagement.Services.Operations.Storage;
using System.Security.Claims;
using Operation = MMD.TenantManagement.Contracts.Operations.Operation;

namespace MMD.TenantManagement.Services.Operations.Execution.Types;

public sealed class CreateWindowsHealthMonitoringDeviceConfiguration : OperationBase, IOperation
{
    public OperationType Type => OperationType.CreateWindowsHealthMonitoringProfile;
    public OperationPrecedence Precedence => OperationPrecedence.Create;
    public OperationPriority Priority => ParseEnumIfExists(OperationPriority.Normal);
    private string DisplayName => this.ParseStringIfExists();
    private string Description => this.ParseStringIfExists();
    private Enablement AllowDeviceHealthMonitoring => this.ParseEnumIfExists(Enablement.Enabled);
    private WindowsHealthMonitoringScope ConfigDeviceHealthMonitoringScope => this.ParseEnumIfExists(WindowsHealthMonitoringScope.BootPerformance | WindowsHealthMonitoringScope.WindowsUpdates);

    public CreateWindowsHealthMonitoringDeviceConfiguration(
        ITelemetryService telemetryService,
        IModificationsRepository modificationsRepository,
        IGraphClientResolver graphClientResolver,
        Operation operation) : base(telemetryService, modificationsRepository, null, graphClientResolver, null, null, operation)
    {
    }

    public async Task ExecuteAsync(Guid tenantId, ClaimsPrincipal executeAsUser = null)
    {
        Guard.NotDefault(tenantId, nameof(tenantId));
        Guard.NotNullOrWhiteSpace(DisplayName, nameof(DisplayName));
        Guard.NotNullOrWhiteSpace(Description, nameof(Description));

        using var telemetryOperation = ResolveTelemetryOperation(tenantId, nameof(CreateWindowsHealthMonitoringDeviceConfiguration), nameof(ExecuteAsync));
        var graph = await ResolveGraphServiceClientForTenantAsync(tenantId, executeAsUser);

        if ((await graph.DeviceManagement.DeviceConfigurations
            .Request()
            .FilterOnEscapedDisplayName(DisplayName)
            .GetAsync())
            .FirstOrDefault() is not WindowsHealthMonitoringConfiguration windowsHealthMonitoringConfiguration)
        {
            var newWindowsHealthMonitoringConfiguration = new WindowsHealthMonitoringConfiguration
            {
                DisplayName = this.DisplayName,
                Description = this.Description,
                AllowDeviceHealthMonitoring = this.AllowDeviceHealthMonitoring,
                ConfigDeviceHealthMonitoringScope = this.ConfigDeviceHealthMonitoringScope
            };

            windowsHealthMonitoringConfiguration = await graph.DeviceManagement.DeviceConfigurations
                .Request()
                .AddAsync(newWindowsHealthMonitoringConfiguration) as WindowsHealthMonitoringConfiguration;
            telemetryOperation.TrackTrace($"Windows Health Monitoring Policy {newWindowsHealthMonitoringConfiguration.DisplayName} was created with Id {newWindowsHealthMonitoringConfiguration.Id} in tenant {tenantId}");
        }
        else
        {
            var updateWindowsHealthMonitoringConfiguration = new WindowsHealthMonitoringConfiguration
            {
                Id = windowsHealthMonitoringConfiguration.Id,
                DisplayName = this.DisplayName,
                Description = this.Description,
                AllowDeviceHealthMonitoring = this.AllowDeviceHealthMonitoring,
                ConfigDeviceHealthMonitoringScope = this.ConfigDeviceHealthMonitoringScope
            };

            await graph.DeviceManagement.DeviceConfigurations[updateWindowsHealthMonitoringConfiguration.Id]
                .Request()
                .UpdateAsync(updateWindowsHealthMonitoringConfiguration);
            telemetryOperation.TrackTrace($"Windows Health Monitoring Policy {updateWindowsHealthMonitoringConfiguration.DisplayName} with Id {updateWindowsHealthMonitoringConfiguration.Id} was updated in tenant {tenantId}");
        }

        CreateOrUpdateOperationProperty("ObjectId", windowsHealthMonitoringConfiguration.Id);
        CreateOrUpdateOperationProperty("ConfigurationVersion", windowsHealthMonitoringConfiguration.Version);
        CreateOrUpdateOperationProperty("LastModifiedDateTimeUtc", windowsHealthMonitoringConfiguration.LastModifiedDateTime ?? DateTime.MinValue);
    }

    public async Task RevertAsync(Guid tenantId, ClaimsPrincipal executeAsUser = null)
    {
        Guard.NotDefault(tenantId, nameof(tenantId));
        Guard.NotNullOrWhiteSpace(DisplayName, nameof(DisplayName));

        using var telemetryOperation = ResolveTelemetryOperation(tenantId, nameof(CreateWindowsHealthMonitoringDeviceConfiguration), nameof(RevertAsync));
        var graph = await ResolveGraphServiceClientForTenantAsync(tenantId, executeAsUser);

        if ((await graph.DeviceManagement.DeviceConfigurations
            .Request()
            .FilterOnEscapedDisplayName(DisplayName)
            .GetAsync())
            .FirstOrDefault() is WindowsHealthMonitoringConfiguration windowsHealthMonitoringConfiguration)
        {
            await graph.DeviceManagement.DeviceConfigurations[windowsHealthMonitoringConfiguration.Id]
                .Request()
                .DeleteAsync();
            telemetryOperation.TrackTrace($"Windows Health Monitoring Policy {windowsHealthMonitoringConfiguration.DisplayName} with Id {windowsHealthMonitoringConfiguration.Id} was deleted from tenant {tenantId}");
        }
        else
        {
            telemetryOperation.TrackTrace($"Windows Health Monitoring Policy {DisplayName} does not exist in tenant {tenantId}, which is okay.");
        }
    }

    public async Task<bool> ValidateAsync(Guid tenantId)
    {
        Guard.NotDefault(tenantId, nameof(tenantId));
        Guard.NotNullOrWhiteSpace(DisplayName, nameof(DisplayName));

        using var telemetryOperation = ResolveTelemetryOperation(tenantId, nameof(CreateWindowsHealthMonitoringDeviceConfiguration), nameof(ValidateAsync));
        var graph = await ResolveGraphServiceClientForTenantAsync(tenantId);

        if ((await graph.DeviceManagement.DeviceConfigurations
            .Request()
            .FilterOnEscapedDisplayName(DisplayName)
            .GetAsync())
            .FirstOrDefault() is not WindowsHealthMonitoringConfiguration windowsHealthMonitoringConfiguration)
        {
            return false;
        }

        return windowsHealthMonitoringConfiguration.AllowDeviceHealthMonitoring == this.AllowDeviceHealthMonitoring
            && windowsHealthMonitoringConfiguration.ConfigDeviceHealthMonitoringScope == this.ConfigDeviceHealthMonitoringScope;
    }
}
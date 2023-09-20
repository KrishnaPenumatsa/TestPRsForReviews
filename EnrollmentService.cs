using Microsoft.mWaaS.Common;
using Microsoft.mWaaS.Services.Core.Azure;
using Microsoft.mWaaS.Services.Core.Devices;
using Microsoft.mWaaS.Services.Core.Domain.Customers;
using Microsoft.mWaaS.Services.Core.Domain.Data;
using Microsoft.mWaaS.Services.Core.Domain.Data.Enrollment;
using Microsoft.mWaaS.Services.Core.Domain.Partners;
using Microsoft.mWaaS.Services.Core.Domain.Services.Credentials;
using Microsoft.mWaaS.Services.Core.Domain.Services.Messages;
using Microsoft.mWaaS.Services.Core.Domain.Services.WindowsHealthMonitoringService;
using Microsoft.mWaaS.Services.Core.Dynamics;
using Microsoft.mWaaS.Services.Core.Dynamics.Contracts;
using Microsoft.mWaaS.Services.Core.ExpectedState;
using Microsoft.mWaaS.Services.Core.Flighting;
using Microsoft.mWaaS.Services.Core.LocationService;
using Microsoft.mWaaS.Services.Core.M365EventAuthoring;
using Microsoft.mWaaS.Services.Core.Operations;
using Microsoft.mWaaS.Services.Core.Profiles;
using Microsoft.mWaaS.Services.Core.TenantManagement;
using Microsoft.mWaaS.Services.Core.Utilities;
using Microsoft.mWaaS.Services.Core.Web;
using Microsoft.mWaaS.Services.Graph;
using MMD.Services.Contracts.Common.Features;
using MMD.Services.Contracts.Common.Plan;
using MMD.Services.Contracts.Common.Tenant;
//using MMD.Services.Contracts.Extensions;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Security.Claims;
using System.Threading.Tasks;
using static Microsoft.mWaaS.Services.Core.Domain.EnrollmentConfiguration;

namespace Microsoft.mWaaS.Services.Core.Domain.Services
{
    /// <inheritdoc />
    public class EnrollmentService : IEnrollmentService
    {
        private readonly IAccountServiceFactory accountServiceFactory;
        private readonly IEnrollmentConfigurationRepository enrollmentConfigurationRepository;
        private readonly ISecurityBaselineRepository securityBaselineRepository;
        private readonly IGroupServiceFactory groupServiceFactory;
        private readonly IMdmServiceFactory mdmServiceFactory;
        private readonly IAutopilotProfileServiceFactory autopilotServiceFactory;
        private readonly IPasswordService passwordService;
        private readonly ITenantShardResolver tenantShardResolver;
        private readonly IICMService icmService;
        private readonly IAppServiceFactory appServiceFactory;
        private readonly ITenantSettingService tenantSettingService;
        private readonly ITelemetryAdapter telemetryClient;
        private readonly Func<ClaimsPrincipal, Task<string>> msGraphAuthenticationCallback;
        private readonly Func<bool, Task<string>> msGraphAppOnlyAuthenticationCallback;
        private readonly Func<Task<string>> m365EventAuthoringAuthenticationCallback;
        private readonly IM365EventAuthoringGatewayFactory m365EventAuthoringGatewayFactory;
        private readonly IOnboardToM365HealthDashboardServiceFactory onboardToM365HealthDashboardServiceFactory;
        private readonly ITenantDataService tenantDataService;
        private readonly IUpdatesRepository updatesRepository;
        private readonly IWin32AppServiceFactory win32AppServiceFactory;
        private readonly IProfilesFacade profilesFacade;
        private readonly IUpdatePolicyServiceFactory updatePolicyServiceFactory;
        private readonly IConditionalAccessService conditionalAccessService;
        private readonly IDeviceInformationResolver deviceInformationResolver;
        private readonly IApiServiceConfig serviceConfig;
        private readonly IDynamicsGateway dynamicsGateway;
        private readonly IWindowsHealthMonitoringService windowsHealthMonitoringService;
        private readonly IAzureQueueStorageManager queueStorageManager;
        private readonly IExpectedStateService expectedStateService;
        private readonly ITenantManagementGateway tenantManagementGateway;
        private readonly IFlightingResolver flightingResolver;
        private readonly ITenantCredentialResolver tenantCredentialResolver;
        private readonly ILocationServiceGatewayFactory locationServiceGatewayFactory;

        private readonly Features defaultFlags = Features.ConfigurableSettingsBlade | Features.MessagesBlade | Features.InTuneAppsGetHelp | Features.PasswordUpdateOnAccess | Features.DeviceRegistration | Features.UpdatePoliciesV2 | Features.AutopilotDeviceRegistration;

        /// <summary>
        ///     Initializes a new instance of the <see cref="EnrollmentService"/>
        ///     class.
        /// </summary>
        public EnrollmentService(
            IEnrollmentConfigurationRepository enrollmentConfigurationRepository,
            ISecurityBaselineRepository securityBaselineRepository,
            IAccountServiceFactory accountServiceFactory,
            IGroupServiceFactory groupServiceFactory,
            IMdmServiceFactory mdmServiceFactory,
            IPasswordService passwordService,
            IAutopilotProfileServiceFactory autopilotServiceFactory,
            ITelemetryAdapter telemetryClient,
            ITenantShardResolver tenantShardResolver,
            IICMService icmService,
            IAppServiceFactory appServiceFactory,
            ITenantSettingService tenantSettingService,
            Func<ClaimsPrincipal, Task<string>> msGraphAuthenticationCallback,
            Func<bool, Task<string>> msGraphAppOnlyAuthenticationCallback,
            Func<Task<string>> m365EventAuthoringAuthenticationCallback,
            IM365EventAuthoringGatewayFactory m365EventAuthoringGatewayFactory,
            IOnboardToM365HealthDashboardServiceFactory onboardToM365HealthDashboardServiceFactory,
            ITenantDataService tenantDataService,
            IUpdatesRepository updatesRepository,
            IWin32AppServiceFactory win32AppServiceFactory,
            IProfilesFacade profilesFacade,
            IConditionalAccessService conditionalAccessService,
            IDeviceInformationResolver deviceInformationResolver,
            IUpdatePolicyServiceFactory updatePolicyServiceFactory,
            IApiServiceConfig serviceConfig,
            IDynamicsGateway dynamicsGateway,
            IWindowsHealthMonitoringService windowsHealthMonitoringService,
            IAzureQueueStorageManager queueStorageManager,
            IExpectedStateService expectedStateService,
            ITenantManagementGateway tenantManagementGateway,
            IFlightingResolver flightingResolver,
            ITenantCredentialResolver tenantCredentialResolver,
            ILocationServiceGatewayFactory locationServiceGatewayFactory)
        {
            Guard.NotNull(enrollmentConfigurationRepository, nameof(enrollmentConfigurationRepository));
            Guard.NotNull(securityBaselineRepository, nameof(securityBaselineRepository));
            Guard.NotNull(accountServiceFactory, nameof(accountServiceFactory));
            Guard.NotNull(groupServiceFactory, nameof(groupServiceFactory));
            Guard.NotNull(autopilotServiceFactory, nameof(autopilotServiceFactory));
            Guard.NotNull(mdmServiceFactory, nameof(mdmServiceFactory));
            Guard.NotNull(passwordService, nameof(passwordService));
            Guard.NotNull(telemetryClient, nameof(telemetryClient));
            Guard.NotNull(tenantShardResolver, nameof(tenantShardResolver));
            Guard.NotNull(msGraphAuthenticationCallback, nameof(msGraphAuthenticationCallback));
            Guard.NotNull(msGraphAppOnlyAuthenticationCallback, nameof(msGraphAppOnlyAuthenticationCallback));
            Guard.NotNull(m365EventAuthoringAuthenticationCallback, nameof(m365EventAuthoringAuthenticationCallback));
            Guard.NotNull(m365EventAuthoringGatewayFactory, nameof(m365EventAuthoringGatewayFactory));
            Guard.NotNull(onboardToM365HealthDashboardServiceFactory, nameof(onboardToM365HealthDashboardServiceFactory));
            Guard.NotNull(appServiceFactory, nameof(appServiceFactory));
            Guard.NotNull(tenantSettingService, nameof(tenantSettingService));
            Guard.NotNull(icmService, nameof(icmService));
            Guard.NotNull(tenantDataService, nameof(tenantDataService));
            Guard.NotNull(updatesRepository, nameof(updatesRepository));
            Guard.NotNull(profilesFacade, nameof(profilesFacade));
            Guard.NotNull(conditionalAccessService, nameof(conditionalAccessService));
            Guard.NotNull(updatePolicyServiceFactory, nameof(updatePolicyServiceFactory));
            Guard.NotNull(serviceConfig, nameof(serviceConfig));
            Guard.NotNull(deviceInformationResolver, nameof(deviceInformationResolver));
            Guard.NotNull(dynamicsGateway, nameof(dynamicsGateway));
            Guard.NotNull(windowsHealthMonitoringService, nameof(windowsHealthMonitoringService));
            Guard.NotNull(queueStorageManager, nameof(queueStorageManager));
            Guard.NotNull(expectedStateService, nameof(expectedStateService));
            Guard.NotNull(tenantManagementGateway, nameof(tenantManagementGateway));
            Guard.NotNull(flightingResolver, nameof(flightingResolver));
            Guard.NotNull(tenantCredentialResolver, nameof(tenantCredentialResolver));
            Guard.NotNull(locationServiceGatewayFactory, nameof(locationServiceGatewayFactory));

            this.enrollmentConfigurationRepository = enrollmentConfigurationRepository;
            this.securityBaselineRepository = securityBaselineRepository;
            this.accountServiceFactory = accountServiceFactory;
            this.groupServiceFactory = groupServiceFactory;
            this.autopilotServiceFactory = autopilotServiceFactory;
            this.mdmServiceFactory = mdmServiceFactory;
            this.passwordService = passwordService;
            this.telemetryClient = telemetryClient;
            this.tenantShardResolver = tenantShardResolver;
            this.msGraphAuthenticationCallback = msGraphAuthenticationCallback;
            this.msGraphAppOnlyAuthenticationCallback = msGraphAppOnlyAuthenticationCallback;
            this.m365EventAuthoringAuthenticationCallback = m365EventAuthoringAuthenticationCallback;
            this.m365EventAuthoringGatewayFactory = m365EventAuthoringGatewayFactory;
            this.onboardToM365HealthDashboardServiceFactory = onboardToM365HealthDashboardServiceFactory;
            this.appServiceFactory = appServiceFactory;
            this.tenantSettingService = tenantSettingService;
            this.icmService = icmService;
            this.tenantDataService = tenantDataService;
            this.updatesRepository = updatesRepository;
            this.win32AppServiceFactory = win32AppServiceFactory;
            this.profilesFacade = profilesFacade;
            this.conditionalAccessService = conditionalAccessService;
            this.deviceInformationResolver = deviceInformationResolver;
            this.updatePolicyServiceFactory = updatePolicyServiceFactory;
            this.serviceConfig = serviceConfig;
            this.dynamicsGateway = dynamicsGateway;
            this.windowsHealthMonitoringService = windowsHealthMonitoringService;
            this.queueStorageManager = queueStorageManager;
            this.expectedStateService = expectedStateService;
            this.tenantManagementGateway = tenantManagementGateway;
            this.flightingResolver = flightingResolver;
            this.tenantCredentialResolver = tenantCredentialResolver;
            this.locationServiceGatewayFactory = locationServiceGatewayFactory;
        }

        public async Task<List<OperationData>> MigrateTenantAsync(
            CustomerTenant tenant,
            ClaimsPrincipal identity)
        {
            string user = identity.ToUpn();
            var eventProperties = new Dictionary<string, string>()
            {
               { "UserUPN", user },
               { "Tenant", tenant.Domain },
               { "tenantId", tenant.DirectoryId.ToString() }
            };

            using (var ow = new OperationWrapper(this.telemetryClient, "MigrateTenantAsync", tenant.DirectoryId.ToString(), tenant.Domain, string.Empty, eventProperties))
            {
                var shard = await this.tenantShardResolver.GetShardAsync(tenant.DirectoryId);

                await ValidateTenantCredentialStorageAsync(shard, new AdminTenant { DirectoryId = tenant.DirectoryId, Domain = tenant.Domain });

                IOperationFactory operationFactory = await this.SetupMigration(tenant, identity, shard);

                var operationsCompleted = new List<OperationData>();

                IEnumerable<IOperation> operationsToExecute;

                // Determine if there are already pending operations on the tenant and execute those.
                IEnumerable<OperationData> operations = (await shard.CustomerTenantRepository.GetTenantOperationsAsync())
                    .Where(x => x.Category == OperationCategory.EnvironmentMigration || x.Category == OperationCategory.Support);

                // Some onboarding operations have already taken place. Retry the ones that failed and execute the rest.
                operationsToExecute = operations
                    .Where(x => x.Status != OperationStatus.Complete && x.Status != OperationStatus.Rejected && x.Status != OperationStatus.Reverted && x.Status != OperationStatus.Skipped)
                    .Select(y => operationFactory.CreateOperation(y)).OrderBy(z => z.Precedence);

                // Execute all the operations.
                foreach (IOperation op in operationsToExecute)
                {
                    await op.Execute(user);
                    operationsCompleted.Add(op.Operation);
                }

                return operationsCompleted;
            }
        }

        public async Task<List<OperationData>> UpdateTenantAsync(
            CustomerTenant tenant,
            ClaimsPrincipal identity,
            DeployableRing ring,
            Func<DeploymentTrackerInput, Task<DeploymentTracker>> createTracker = null)
        {
            return await UpdateTenantAsyncInternal(null, tenant, identity, ring, createTracker, true);
        }

        public async Task<List<OperationData>> UpdateTenantAsyncWithToken(
            Func<bool, Task<string>> graphTokenDelegate,
            CustomerTenant tenant,
            ClaimsPrincipal identity,
            DeployableRing ring,
            Func<DeploymentTrackerInput, Task<DeploymentTracker>> createTracker,
            bool needCompareCurrentAndTarget = true)
        {
            return await UpdateTenantAsyncInternal(graphTokenDelegate, tenant, identity, ring, createTracker, needCompareCurrentAndTarget);
        }

        public async Task<OnboardingOperationsStatus> GetOnboardingOperationsStatus(Guid tenantId)
        {
            var properties = new Dictionary<string, string>()
            {
                { "TenantId", tenantId.ToString() }
            };

            using (var ow = new OperationWrapper(this.telemetryClient, "GetOnboardingOperationsStatus", tenantId.ToString(), "", string.Empty, properties))
            {
                try
                {
                    return await this.tenantManagementGateway.GetOnboardingStatusAsync(tenantId);
                }
                catch (Exception e)
                {
                    ow.TrackTrace($"Failed to retrieve onboarding operations status. Tenant - {tenantId}");
                    ow.TrackException(e);
                    throw;
                }
            }
        }

        private async Task<List<OperationData>> UpdateTenantAsyncInternal(
            Func<bool, Task<string>> graphTokenDelegate,
            CustomerTenant tenant,
            ClaimsPrincipal identity,
            DeployableRing ring,
            Func<DeploymentTrackerInput, Task<DeploymentTracker>> createTracker,
            bool needCompareCurrentAndTarget = true)
        {
            var shard = await this.tenantShardResolver.GetShardAsync(tenant.DirectoryId);
            TenantConfigurationStatus tcs = await shard.CustomerTenantRepository.GetDeploymentRingConfigStatus();

            var properties = new Dictionary<string, string>()
            {
                { "TenantName", tenant.Domain },
                { "TenantId", tenant.DirectoryId.ToString() },
                { "CurrentTestVersion", tcs.Test.CurrentConfigurationVersion.ToString() },
                { "TargetTestVersion", tcs.Test.TargetConfigurationVersion.ToString() },
                { "CurrentFirstVersion", tcs.First.CurrentConfigurationVersion.ToString() },
                { "TargetFirstVersion", tcs.First.TargetConfigurationVersion.ToString() },
                { "CurrentFastVersion", tcs.Fast.CurrentConfigurationVersion.ToString() },
                { "TargetFastVersion", tcs.Fast.TargetConfigurationVersion.ToString() },
                { "CurrentBroadVersion", tcs.Broad.CurrentConfigurationVersion.ToString() },
                { "TargetBroadVersion", tcs.Broad.TargetConfigurationVersion.ToString() },
                { "DeploymentRing", ring.ToString() }
            };

            using (var ow = new OperationWrapper(this.telemetryClient, "UpdateTenantAsync", tenant.DirectoryId.ToString(), tenant.Domain, string.Empty, properties))
            {
                double currentRingVersion = tcs.GetCurrentVersionForRing(ring);
                double targetRingVersion = tcs.GetTargetVersionForRing(ring);

                if (needCompareCurrentAndTarget && currentRingVersion == targetRingVersion)
                {
                    string failureMessage = $"Tenant is already on the latest version for ring: {ring}";
                    properties.Add("FailureMessage", failureMessage);
                    ow.TrackEvent("ApproveConfigurationVersionUpgradeFailure", properties);
                    throw new InvalidOperationException(failureMessage);
                }

                await ValidateTenantCredentialStorageAsync(shard, new AdminTenant { DirectoryId = tenant.DirectoryId, Domain = tenant.Domain });

                IOperationFactory operationFactory;
                if (graphTokenDelegate.IsNull())
                {
                    operationFactory = await this.SetupMigration(tenant, identity, shard);
                }
                else
                {
                    operationFactory = await this.SetupMigrationWithToken(graphTokenDelegate, tenant, identity, shard);
                }
                string user = identity.ToUpn();
                var operationsCompleted = new List<OperationData>();

                // Determine if there are already pending operations on the tenant and execute those.
                var operations = await shard.CustomerTenantRepository.GetPendingTenantOperationsAsync(OperationCategory.ConfigurationVersionUpgrade, null);
                var operationsToExecute = operations
                    .Where(x => (int)x.Ring == (int)ring
                        || (ring is DeployableRing.Test && x.Ring is OperationRing.Global))
                    .Select(x => operationFactory.CreateOperation(x))
                    .OrderBy(x => x.Precedence)
                    .ToList();
                if (needCompareCurrentAndTarget)
                {
                    ow.TrackTrace($"There are {operationsToExecute.Count} operations required to update {tenant.Domain} from version {currentRingVersion} to {targetRingVersion} in the {ring} ring");
                }
                else
                {
                    ow.TrackTrace($"There are {operationsToExecute.Count} operations required to update {tenant.Domain} from version -1(currentRingVersion unknown) to {targetRingVersion} in the {ring} ring");
                }

                if (!operationsToExecute.Any())
                {
                    ow.TrackEvent("UpdateTenantAsync-NoOperations");
                }

                // Execute all the operations.
                foreach (IOperation op in operationsToExecute)
                {
                    await op.Execute(user);
                    operationsCompleted.Add(op.Operation);
                }

                if (needCompareCurrentAndTarget)
                {
                    await shard.CustomerTenantRepository.UpdateTenantConfigurationVersionAsync(targetRingVersion, ring);
                }

                await InitiateDeploymentTracker(tenant, operationsCompleted, ring, graphTokenDelegate, targetRingVersion, createTracker, ow);

                return operationsCompleted;
            }
        }

        private async Task InitiateDeploymentTracker(
            CustomerTenant tenant,
            List<OperationData> completedOperations,
            DeployableRing ring,
            Func<bool, Task<string>> graphTokenDelegate,
            double correlationId,
            Func<DeploymentTrackerInput, Task<DeploymentTracker>> createTracker,
            OperationWrapper ow)
        {
            ow.TrackTrace($"Attempting to create a {DeploymentSource.ConfigurationVersionUpgrade} deployment tracker for Tenant - {tenant.DirectoryId}, Ring - {ring}, Config Id - {correlationId}");
            try
            {
                Guard.NotNull(createTracker, nameof(createTracker));
                var entities = GetEntities(completedOperations);

                await createTracker(
                    new DeploymentTrackerInput
                    {
                        TenantId = tenant.DirectoryId,
                        Source = DeploymentSource.ConfigurationVersionUpgrade,
                        Ring = ring.ToString(),
                        ExternalCorrelationId = correlationId.ToString(),
                        IcmTicket = 1,
                        SuccessCriteriaId = 1,
                        GraphTokenDelegate = graphTokenDelegate,
                        Entities = entities
                    });
            }
            catch (Exception e)
            {
                ow.TrackTrace($"Unable to create deployment tracker for {DeploymentSource.ConfigurationVersionUpgrade}. Tenant - {tenant.DirectoryId}, Ring - {ring}, Config Id - {correlationId}");
                ow.TrackException(e);
            }
        }

        private List<DeploymentTrackerEntity> GetEntities(List<OperationData> operations)
        {
            var entities = new List<DeploymentTrackerEntity>();
            foreach (var operation in operations)
            {
                if (operation.Type.Equals(OperationType.AssignPoliciesToGroup))
                {
                    if (operation.Status.Equals(OperationStatus.Complete))
                    {
                        entities.Add(new DeploymentTrackerEntity() { EntityType = EntityType.Policy, EntityName = operation.Properties["policies"] });
                    }
                }
                else if (operation.Type.Equals(OperationType.AssignPowershellToGroup))
                {
                    if (operation.Status.Equals(OperationStatus.Complete))
                    {
                        entities.Add(new DeploymentTrackerEntity() { EntityType = EntityType.Powershell, EntityName = operation.Properties["PowershellConfigurationName"] });
                    }
                }
            }
            return entities;
        }

        public async Task<OperationData> MigrateSingleOperationAsync(CustomerTenant tenant, ClaimsPrincipal identity, OperationData operation, Func<bool, Task<string>> graphTokenDelegate = null)
        {
            var user = identity.ToUpn();
            var eventProperties = new Dictionary<string, string>()
            {
               { "UserUPN", user },
               { "Tenant", tenant.Domain },
               { "tenantId", tenant.DirectoryId.ToString() }
            };

            using (var operationWrapper = new OperationWrapper(this.telemetryClient, "MigrateSingleOperationAsync", tenant.DirectoryId.ToString(), tenant.Domain, string.Empty, eventProperties))
            {
                var shard = await this.tenantShardResolver.GetShardAsync(tenant.DirectoryId);

                await ValidateTenantCredentialStorageAsync(shard, new AdminTenant { DirectoryId = tenant.DirectoryId, Domain = tenant.Domain });

                IOperationFactory operationFactory;
                if (graphTokenDelegate != null)
                {
                    operationFactory = await this.SetupMigrationWithToken(graphTokenDelegate, tenant, identity, shard);
                }
                else
                {
                    operationFactory = await this.SetupMigration(tenant, identity, shard);
                }

                IOperation operationToExecute = operationFactory.CreateOperation(operation);
                await operationToExecute.Execute(user);
                return operationToExecute.Operation;
            }
        }

        public async Task<List<OperationData>> ExecuteManuallyOperationsAsync(
            Func<bool, Task<string>> graphTokenDelegate,
            CustomerTenant tenant,
            IEnumerable<OperationData> scheduled,
            ClaimsPrincipal identity)
        {
            Guard.NotNull(tenant, nameof(tenant));
            Guard.NotNull(identity, nameof(identity));

            var user = identity.ToUpn();
            var eventProperties = new Dictionary<string, string>()
            {
                { "UserUPN", user},
                { "Tenant", tenant.Domain },
                { "TenantId", tenant.DirectoryId.ToString() }
            };

            using (var operationWrapper = new OperationWrapper(this.telemetryClient, "ExecuteManuallyOperationsAsync", tenant.DirectoryId.ToString(), tenant.Domain, string.Empty, eventProperties))
            {
                var operationFactory = await this.PrepareOperationsExecutionAsync(graphTokenDelegate, tenant, identity, operationWrapper);
                var operationsCompleted = new List<OperationData>();

                // Determine if there are already pending operations on the tenant and execute those.
                if (scheduled.Any())
                {
                    operationWrapper.TrackTrace("ExecuteOperationsAsyncInternal");
                    // Execute the operations that are ready to be executed in the order of their precedence.
                    var operationsToExecute = scheduled
                        .Select(y => operationFactory.CreateOperation(y))
                        .OrderBy(z => z.Precedence);
                    foreach (OperationShell os in operationsToExecute)
                    {
                        await os.Execute(user);
                        operationsCompleted.Add(os.Operation);
                    }
                }
                return operationsCompleted;
            }
        }

        public async Task<List<OperationData>> ExecuteOnboardingOperationsAsync(
            Func<bool, Task<string>> graphTokenDelegate,
            CustomerTenant tenant,
            IEnumerable<OperationData> scheduled,
            ClaimsPrincipal identity,
            int remainingOperationsInTenantManagement)
        {
            Guard.NotNull(tenant, nameof(tenant));
            Guard.NotNull(identity, nameof(identity));
            Guard.NotNull(graphTokenDelegate, nameof(graphTokenDelegate));

            var user = identity.ToUpn();
            var eventProperties = new Dictionary<string, string>()
            {
                { "UserUPN", user},
                { "Tenant", tenant.Domain },
                { "TenantId", tenant.DirectoryId.ToString() }
            };

            using (var operationWrapper = new OperationWrapper(this.telemetryClient, "ExecuteOnboardingOperationsAsync", tenant.DirectoryId.ToString(), tenant.Domain, string.Empty, eventProperties))
            {
                var operationFactory = await this.PrepareOperationsExecutionAsync(graphTokenDelegate, tenant, identity, operationWrapper);
                var operationsCompleted = new List<OperationData>();

                // Determine if there are already pending operations on the tenant and execute those.
                if (scheduled.Any())
                {
                    operationWrapper.TrackTrace("ExecuteOnboardingOperationsAsyncInternal");
                    // Execute the operations that are ready to be executed in the order of their precedence.
                    var operationsToExecute = scheduled
                        .Where(x => x.Category is OperationCategory.Onboarding)
                        .Select(y => operationFactory.CreateOperation(y))
                        .OrderBy(z => z.Precedence)
                        .ToList();
                    foreach (OperationShell os in operationsToExecute)
                    {
                        if (await os.ExecuteWithoutFailing(user))
                        {
                            operationsCompleted.Add(os.Operation);
                        }
                    }

                    // We do not need to potentially update the tenant if the operations to execute did not all complete.
                    if (operationsCompleted.Count == operationsToExecute.Count && remainingOperationsInTenantManagement is 0)
                    {
                        operationWrapper.TrackTrace("All operations scheduled were completed - Check if we can update the tenant status");
                        await this.CheckTenantEnrolledStateAsync(tenant.DirectoryId, operationWrapper);
                    }
                }

                return operationsCompleted;
            }
        }

        private async Task<IOperationFactory> PrepareOperationsExecutionAsync(
            Func<bool, Task<string>> graphTokenDelegate,
            CustomerTenant tenant,
            ClaimsPrincipal identity,
            OperationWrapper ow)
        {
            ow.TrackTrace("PrepareOperationsExecutionAsync");
            var shard = await this.tenantShardResolver.GetShardAsync(tenant.DirectoryId);

            await ValidateTenantCredentialStorageAsync(shard, new AdminTenant { DirectoryId = tenant.DirectoryId, Domain = tenant.Domain });

            IOperationFactory operationFactory = await this.SetupMigrationWithToken(graphTokenDelegate, tenant, identity, shard);
            ow.TrackTrace("Successfully created operation factory");
            return operationFactory;
        }

        public async Task CheckTenantEnrolledStateAsync(Guid tenantId, OperationWrapper ow)
        {
            var shard = await this.tenantShardResolver.GetShardAsync(tenantId);
            var operations = await shard.CustomerTenantRepository.GetTenantOperationsAsync();
            var onboardingOperations = operations
                .Where(x => x.Category is OperationCategory.OnboardingCritical || x.Category is OperationCategory.Onboarding)
                .ToList();
            var onboardingCompleteOperations = onboardingOperations
                .Where(x => x.Status is OperationStatus.Complete)
                .ToList();
            var onboardingSkippedOperations = onboardingOperations
                .Where(x => x.Status is OperationStatus.Skipped)
                .ToList();

            // "onboardingRevertedOperations" could be non-zero if the service deploys a config release that reverts
            // onboarding critical operations from the LKG config while the tenant is PartiallyEnrolled.
            var onboardingRevertedOperations = onboardingOperations
                .Where(x => x.Status is OperationStatus.Reverted)
                .ToList();

            ow.TrackTrace($"Onboarding operations (completed + skipped + reverted)/total: ({onboardingCompleteOperations.Count} + {onboardingSkippedOperations.Count} + {onboardingRevertedOperations.Count})/{onboardingOperations.Count}");

            var enrolled = false;
            var planOperations = await enrollmentConfigurationRepository.GetPlansOperations();
            var customerTenant = await shard.CustomerTenantRepository.FindTenantAsync();

            var plans = customerTenant.GetPartialOrFailedEnrolledPlans();
            foreach (var plan in plans)
            {
                var correlationIds = planOperations
                    .Where(p => p.PlanId == plan)
                    .Select(p => p.CorrelationId)
                    .ToHashSet();
                var onboardingOperationsSpecifiedPlanCount = onboardingOperations
                    .Where(o => correlationIds.Contains(o.CorrelationId))
                    .Count();
                var onboardingCompleteOperationsSpecifiedPlanCount = onboardingCompleteOperations
                    .Where(o => correlationIds.Contains(o.CorrelationId))
                    .Count();
                var onboardingSkippedOperationsSpecifiedPlanCount = onboardingSkippedOperations
                    .Where(o => correlationIds.Contains(o.CorrelationId))
                    .Count();
                var onboardingRevertedOperationsSpecifiedPlanCount = onboardingRevertedOperations
                    .Where(o => correlationIds.Contains(o.CorrelationId))
                    .Count();

                ow.TrackTrace($"Onboarding operations of {plan} (completed + skipped + reverted)/total: ({onboardingCompleteOperationsSpecifiedPlanCount} + {onboardingSkippedOperationsSpecifiedPlanCount} + {onboardingRevertedOperationsSpecifiedPlanCount})/{onboardingOperationsSpecifiedPlanCount}");

                enrolled = onboardingOperationsSpecifiedPlanCount == (onboardingCompleteOperationsSpecifiedPlanCount + onboardingSkippedOperationsSpecifiedPlanCount + onboardingRevertedOperationsSpecifiedPlanCount);
                if (enrolled)
                {
                    await shard.CustomerTenantRepository.UpdateTenantStateAsync(TenantState.Enrolled, plan);
                    ow.TrackTrace($"All operations of {plan} completed or skipped - Tenant now enrolled with {plan} plan");

                    ow.TrackTrace($"UpdateCrmTenant to enrolled");
                    await this.dynamicsGateway.UpdateCrmTenant(tenantId, new CrmTenantPatch
                    {
                        State = TenantState.Enrolled.ToString(),
                        EnrolledTimestamp = DateTime.UtcNow,
                        Plan = plan.ToString()
                    });
                }
            }
        }

        private async Task<IOperationFactory> SetupMigrationWithToken(
            Func<bool, Task<string>> graphTokenDelegate,
            CustomerTenant tenant,
            ClaimsPrincipal identity,
            ITenantShard shard)
        {
            Guard.NotNull(tenant, nameof(tenant));
            Guard.NotNull(identity, nameof(identity));
            var operationsCompleted = new List<OperationData>();

            var user = identity.ToUpn();
            var eventProperties = new Dictionary<string, string>()
            {
               { "UserUPN", user}
            };
            using (var operationWrapper = new OperationWrapper(this.telemetryClient, "SetupMigrationWithToken", tenant.DirectoryId.ToString(), tenant.Domain, string.Empty, eventProperties))
            {
                var accountService = this.accountServiceFactory.Create(graphTokenDelegate);
                var autopilotService = this.autopilotServiceFactory.Create(graphTokenDelegate);

                var configuration = await this.enrollmentConfigurationRepository.GetLatestConfigurationAsync();
                Debug.Assert(configuration != null);

                var groupService = this.groupServiceFactory.Create(graphTokenDelegate);
                // Since this is coming from the partner API, we don't have the adGraphToken or omsAccessToken, that's ok as we don't need them at this point.
                var mdmService = this.mdmServiceFactory.Create(graphTokenDelegate);
                var m365EventAuthoringGateway = this.m365EventAuthoringGatewayFactory.Create(
                    () =>
                    {
                        return AuthenticateToM365EventAuthoringAsync();
                    });
                var onboardToM365HealthDashboardService = this.onboardToM365HealthDashboardServiceFactory.Create(m365EventAuthoringGateway);
                IAppService appService = this.appServiceFactory.Create(graphTokenDelegate);
                IWin32AppService win32AppService = this.win32AppServiceFactory.Create(graphTokenDelegate);
                var updatePolicyService = this.updatePolicyServiceFactory.Create(graphTokenDelegate);

                IOperationFactory operationFactory = new OperationFactory(
                    tenant,
                    configuration,
                    shard,
                    mdmService,
                    autopilotService,
                    groupService,
                    accountService,
                    tenantCredentialResolver,
                    passwordService,
                    telemetryClient,
                    securityBaselineRepository,
                    this.icmService,
                    appService,
                    onboardToM365HealthDashboardService,
                    tenantSettingService,
                    updatesRepository,
                    win32AppService,
                    profilesFacade,
                    updatePolicyService,
                    expectedStateService,
                    serviceConfig,
                    flightingResolver);

                return operationFactory;
            }
        }

        private async Task<IOperationFactory> SetupMigration(CustomerTenant tenant, ClaimsPrincipal identity, ITenantShard shard)
        {
            Guard.NotNull(tenant, nameof(tenant));
            Guard.NotNull(identity, nameof(identity));
            var operationsCompleted = new List<OperationData>();

            using (var operationWrapper = new OperationWrapper(this.telemetryClient, "SetupMigration", tenant.DirectoryId.ToString(), tenant.Domain, string.Empty, null))
            {
                var accountService = this.accountServiceFactory.Create((useAppAuth) => this.AuthenticateToMsGraphAsync(identity));
                var autopilotService = this.autopilotServiceFactory.Create((useAppAuth) => this.AuthenticateToMsGraphAsync(identity));

                var configuration = await this.enrollmentConfigurationRepository.GetLatestConfigurationAsync();
                Debug.Assert(configuration != null);

                var groupService = this.groupServiceFactory.Create((useAppAuth) => this.AuthenticateToMsGraphAsync(identity));
                var mdmService = this.mdmServiceFactory.Create((useAppAuth) => this.AuthenticateToMsGraphAsync(identity));
                IAppService appService = this.appServiceFactory.Create((useAppAuth) => this.AuthenticateToMsGraphAsync(identity));
                var m365EventAuthoringGateway = this.m365EventAuthoringGatewayFactory.Create(
                    () =>
                    {
                        return AuthenticateToM365EventAuthoringAsync();
                    });
                var onboardToM365HealthDashboardService = this.onboardToM365HealthDashboardServiceFactory.Create(m365EventAuthoringGateway);
                var win32AppService = this.win32AppServiceFactory.Create((useAppAuth) => this.AuthenticateToMsGraphAsync(identity));
                var updatePolicyService = this.updatePolicyServiceFactory.Create((useAppAuth) => this.AuthenticateToMsGraphAsync(identity));

                IOperationFactory operationFactory = new OperationFactory(
                    tenant,
                    configuration,
                    shard,
                    mdmService,
                    autopilotService,
                    groupService,
                    accountService,
                    tenantCredentialResolver,
                    passwordService,
                    telemetryClient,
                    securityBaselineRepository,
                    this.icmService,
                    appService,
                    onboardToM365HealthDashboardService,
                    tenantSettingService,
                    updatesRepository,
                    win32AppService,
                    profilesFacade,
                    updatePolicyService,
                    expectedStateService,
                    serviceConfig,
                    flightingResolver);

                return operationFactory;
            }
        }

        public async Task<List<OperationData>> CheckPendingOperationsAsync(
            Func<bool, Task<string>> graphTokenDelegate,
            CustomerTenant tenant,
            IEnumerable<OperationData> pending,
            ClaimsPrincipal identity)
        {
            Guard.NotNull(tenant, nameof(tenant));
            Guard.NotNull(identity, nameof(identity));

            var user = identity.ToUpn();
            var eventProperties = new Dictionary<string, string>()
            {
               { "UserUPN", user},
               { "Tenant", tenant.Domain },
               { "tenantId", tenant.DirectoryId.ToString() }
            };

            using (var operationWrapper = new OperationWrapper(this.telemetryClient, "CheckPendingOperationsAsync", tenant.DirectoryId.ToString(), tenant.Domain, string.Empty, eventProperties))
            {
                var shard = await this.tenantShardResolver.GetShardAsync(tenant.DirectoryId);

                await ValidateTenantCredentialStorageAsync(shard, new AdminTenant { DirectoryId = tenant.DirectoryId, Domain = tenant.Domain });

                IOperationFactory operationFactory = await this.SetupMigrationWithToken(graphTokenDelegate, tenant, identity, shard);
                IEnumerable<IOperation> operationsToCheck;
                var operationsChecked = new List<OperationData>();

                // Determine if there are already pending operations on the tenant and check those.
                if (pending.Any())
                {
                    // Check the operations that are ready to be checked in the order of their precedence.
                    operationsToCheck = pending.Select(y => operationFactory.CreateOperation(y)).OrderBy(z => z.Precedence);
                    foreach (OperationShell os in operationsToCheck)
                    {
                        await os.CheckConditionsOperation();
                        operationsChecked.Add(os.Operation);
                    }
                }
                return operationsChecked;
            }
        }

        public async Task SetTenantPreferences(CustomerTenant tenant, ClaimsPrincipal identity, EnrollmentOptions enrollmentOptions)
        {
            Guard.NotNull(tenant, nameof(tenant));
            var eventProperties = new Dictionary<string, string>()
            {
                { "Tenant", tenant.Domain },
                { "tenantId", tenant.DirectoryId.ToString() },
                { "EnableWindowsDeviceLocation", enrollmentOptions.EnableWindowsDeviceLocation.ToString() }
            };
            // Just update consent and not call operation now, because will do it later when enroll.
            using (var operationWrapper = new OperationWrapper(this.telemetryClient, "SetTenantPreferences", tenant.DirectoryId.ToString(), tenant.Domain, string.Empty, eventProperties))
            {
                try
                {
                    await this.UpdateTenantWindowsDeviceLocationConsentAsync(tenant, enrollmentOptions.EnableWindowsDeviceLocation);
                    var shard = await this.tenantShardResolver.GetShardAsync(tenant.DirectoryId);
                    tenant = await shard.CustomerTenantRepository.FindTenantAsync();
                }
                catch (Exception e)
                {
                    var customProperties = new Dictionary<string, string>()
                    {
                        { "Error message", e.Message }
                    };
                    operationWrapper.TrackEvent("Set Tenant Preferences Failure", customProperties);
                    operationWrapper.TrackException(e);
                }
            }
        }

        /// <inheritdoc />
        public async Task EnrollTenantAsync(
            CustomerTenant tenant,
            ClaimsPrincipal identity,
            EnrollmentOptions options,
            PlanType plan)
        {
            Guard.NotNull(tenant, nameof(tenant));
            Guard.NotNull(identity, nameof(identity));

            var user = identity.ToUser();

            var eventProperties = new Dictionary<string, string>()
            {
                { "UserUPN", user.Upn },
                { "Tenant", tenant.Domain },
                { "tenantId", tenant.DirectoryId.ToString() },
                { "Plan", plan.ToString()}
            };
            await this.SetTenantPreferences(tenant, identity, options);

            using (var operationWrapper = new OperationWrapper(this.telemetryClient, "EnrollTenantAsync", tenant.DirectoryId.ToString(), tenant.Domain, string.Empty, eventProperties))
            {
                if (!tenant.IsEligibleToEnroll(plan))
                {
                    throw new TenantAlreadyEnrolledException(tenant.Domain);
                }

                IAccountService accountService;
                IAutopilotProfileService autopilotService;
                ITenantShard shard;
                IGroupService groupService;
                IMdmService mdmService;
                const double lastKnownGoodVersion = 1000;

                try
                {
                    accountService = this.accountServiceFactory.Create((useAppAuth) => this.AuthenticateToMsGraphAsync(identity));
                    autopilotService = this.autopilotServiceFactory.Create((useAppAuth) => this.AuthenticateToMsGraphAsync(identity));
                    shard = await this.tenantShardResolver.GetShardAsync(tenant.DirectoryId);

                    groupService = this.groupServiceFactory.Create((useAppAuth) => this.AuthenticateToMsGraphAsync(identity));
                    mdmService = this.mdmServiceFactory.Create((useAppAuth) => this.AuthenticateToMsGraphAsync(identity));

                    await ValidateTenantCredentialStorageAsync(shard, new AdminTenant { DirectoryId = tenant.DirectoryId, Domain = tenant.Domain });
                }
                catch (Exception e)
                {
                    var customProperties = new Dictionary<string, string>()
                    {
                        { "Error message", e.Message }
                    };
                    operationWrapper.TrackException(e);
                    try
                    {
                        shard = await this.tenantShardResolver.GetShardAsync(tenant.DirectoryId);
                        await shard.CustomerTenantRepository.UpdateTenantStateAsync(TenantState.EnrollmentFailed, plan);
                    }
                    catch (Exception) { }
                    await this.dynamicsGateway.UpdateCrmTenant(tenant.DirectoryId, new CrmTenantPatch()
                    {
                        State = TenantState.EnrollmentFailed.ToString(),
                        Plan = plan.ToString()
                    });
                    throw;
                }

                operationWrapper.TrackTrace("Beginning tenant enrollment");
                await shard.CustomerTenantRepository.UpdateTenantStateAsync(TenantState.Enrolling, plan);

                try
                {
                    var planToEnroll = tenant.GetPlanDTOFromPlanType(plan);
                    var result = await this.tenantManagementGateway.EnrollAsync(tenant, planToEnroll, options, identity);
                }
                catch (Exception e)
                {
                    await shard.CustomerTenantRepository.UpdateTenantStateAsync(TenantState.EnrollmentFailed, plan);
                    var customProperties = new Dictionary<string, string>()
                        {
                            { "Error message", e.Message }
                        };
                    operationWrapper.TrackEvent("Onboarding-Failure", customProperties);
                    operationWrapper.TrackException(e);

                    await this.dynamicsGateway.UpdateCrmTenant(tenant.DirectoryId, new CrmTenantPatch()
                    {
                        State = TenantState.EnrollmentFailed.ToString(),
                        Plan = plan.ToString()
                    });

                    throw;
                }

                var deploymentRings = await GetDeploymentRings(plan);
                // This will create entries in the DeploymentRingsDB which is missing a few datapoints but will be udpated by the
                // dataupdater within 1 hour of enrollment. Creating these now will also get rid of some errors we are seeing due
                // to those not having been created already.
                await shard.CustomerTenantRepository.UpdateTenantDeploymentRingsAsync(deploymentRings);

                if (!tenant.IsEnrolled())
                {
                    await shard.CustomerTenantRepository.UpdateTenantAllRingsConfigurationVersionAsync(lastKnownGoodVersion);
                }
                await shard.CustomerTenantRepository.UpdateTenantFeatureFlags(defaultFlags, true);

                if (plan != PlanType.Starter)
                {
                    // We don't need to exclude our service accounts from Autopatch customer CA policies since we removed all service accounts
                    var modernConditionalAccessPolicyFlightResult = await IsModernConditionalAccessManagementFlightEnabledAsync(tenant.DirectoryId);
                    operationWrapper.TrackTrace($"Modern Conditional Access Flight result for tenant {tenant.DirectoryId}: {modernConditionalAccessPolicyFlightResult}");
                    if (!modernConditionalAccessPolicyFlightResult)
                    {
                        try
                        {
                            var caPolicies = await conditionalAccessService.GetConditionalAccessPolicies(identity);
                            operationWrapper.TrackTrace($"Found {caPolicies.Count()} CA Policies on {tenant.Domain}");
                            var groupIds = await groupService.GetGroupIdsByNameAsync(Constants.MMD_SERVICE_GROUP_NAME);
                            var excludedGroupId = groupIds.FirstOrDefault();
                            var blockingCAPolicies = await conditionalAccessService.CheckBlockingConditionalAccessPoliciesAsync(caPolicies, identity, tenant, excludedGroupId, null, true);
                            await mdmService.ModifyConditionalAcccessPolicies(tenant.DirectoryId.ToString(), blockingCAPolicies);
                            operationWrapper.TrackTrace($"Found {blockingCAPolicies.Count} blocking CA Policies on {tenant.Domain} enrollment. Excluded ourselves from them.");
                        }
                        catch (Exception e)
                        {
                            // Per Zach, we don’t fail here for three main reasons:
                            // 1. there is no UI to remediate, and no one wanted to fund UI to remediate
                            // 2. because mmd had handholding enrollment back in the day, it was easy to get in contact with the customer, which really isn’t the case anymore
                            // 3. there are failure scenarios around legacy conditional access policies that we can’t programmatically make decisions about
                            operationWrapper.AddProperty("CaughtException", e.Message);
                            operationWrapper.TrackTrace($"Could not exclude blocking conditional access policies for {tenant.Domain} enrollment.");
                        }
                    }
                }

                // creates policy for collecting device data
                await this.CreateWindowsHealthMonitoringConfiguration(tenant, (useAppAuth) => this.AuthenticateToMsGraphAsync(identity), options.WindowsHealthMonitoringAssignJustMMDDevices, plan, shard);

                // Update the configuration of rings for this plan if this is not first enroll.
                if (tenant.IsEnrolled())
                {
                    await this.AlignRingConfigurationVersion(tenant, operationWrapper, plan);
                }

                await this.FinalizeEnrollment(shard, tenant.DirectoryId, lastKnownGoodVersion, operationWrapper, plan);
                operationWrapper.TrackTrace("Finalized tenant enrollment");
            }
        }

        /// <inheritdoc />
        public async Task EnrollTenantToPlanAsync(
            Func<bool, Task<string>> graphTokenDelegate,
            CustomerTenant tenant,
            ClaimsPrincipal identity,
            PlanType plan)
        {
            Guard.NotNull(tenant, nameof(tenant));

            var user = identity.ToUser();
            var eventProperties = new Dictionary<string, string>()
            {
                { "UserUPN", user.Upn },
                { "Tenant", tenant.Domain },
                { "tenantId", tenant.DirectoryId.ToString() },
                { "plan", plan.ToString() },
            };

            using (var operationWrapper = new OperationWrapper(this.telemetryClient, nameof(EnrollTenantToPlanAsync), tenant.DirectoryId.ToString(), tenant.Domain, string.Empty, eventProperties))
            {
                ITenantShard shard;
                try
                {
                    shard = await this.tenantShardResolver.GetShardAsync(tenant.DirectoryId);
                    await ValidateTenantCredentialStorageAsync(shard, new AdminTenant { DirectoryId = tenant.DirectoryId, Domain = tenant.Domain });
                }
                catch (Exception e)
                {
                    var customProperties = new Dictionary<string, string>()
                    {
                        { "Error message", e.Message }
                    };
                    operationWrapper.TrackEvent("Onboarding-Failure", customProperties);
                    operationWrapper.TrackException(e);
                    throw;
                }

                operationWrapper.TrackTrace("Beginning enroll tenant to plan");
                await shard.CustomerTenantRepository.UpdateTenantStateAsync(TenantState.Enrolling, plan);

                try
                {
                    var planToEnroll = tenant.GetPlanDTOFromPlanType(plan);
                    var options = new EnrollmentOptions
                    {
                        EnableWindowsDeviceLocation = false,
                        WindowsHealthMonitoringAssignJustMMDDevices = true
                    };
                    var result = await this.tenantManagementGateway.EnrollAsync(tenant, planToEnroll, options, identity);
                }
                catch (Exception e)
                {
                    await shard.CustomerTenantRepository.UpdateTenantStateAsync(TenantState.EnrollmentFailed, plan);
                    var customProperties = new Dictionary<string, string>()
                    {
                       { "Error message", e.Message }
                    };
                    operationWrapper.TrackEvent("Onboarding-Failure", customProperties);
                    operationWrapper.TrackException(e);
                    throw;
                }

                // creates policy to always assign policy to MMD group, since device in MMD new plans always need this policy
                await this.CreateWindowsHealthMonitoringConfiguration(tenant, graphTokenDelegate, true, plan, shard);

                operationWrapper.TrackTrace("Enrollment - FinalizeEnrollNewPlan");
                await shard.CustomerTenantRepository.UpdateTenantStateAsync(TenantState.PartiallyEnrolled, plan);

                // Update the configuration of rings for this plan if this is not first enroll.
                if (tenant.IsEnrolled())
                {
                    await this.AlignRingConfigurationVersion(tenant, operationWrapper, plan);
                }

                operationWrapper.TrackTrace("Finalized tenant new plan enrollment");
            }
        }

        public async Task AlignRingConfigurationVersion(CustomerTenant tenant, OperationWrapper operationWrapper, PlanType plan)
        {
            operationWrapper.TrackTrace("Call function app asynchronously to align ring configuration");
            string updateConfigurationQueueName = "align-configurations-by-plan";
            string message = JsonConvert.SerializeObject(new RingConfigurationAlignMessage
            {
                TenantId = tenant.DirectoryId,
                TenantDomain = tenant.Domain,
                Plan = plan,
            });
            await queueStorageManager.QueueMessage(updateConfigurationQueueName, message);
            operationWrapper.TrackTrace("Ring configuration alignment message queued");
        }

        private async Task FinalizeEnrollment(ITenantShard shard, Guid tenantId, double configVersion, OperationWrapper ow, PlanType plan = PlanType.Premium)
        {
            ow.TrackTrace("Enrollment - FinalizeEnrollment");
            await shard.CustomerTenantRepository.UpdateTenantStateAsync(TenantState.PartiallyEnrolled, plan);

            await this.dynamicsGateway.UpdateCrmTenant(tenantId, new CrmTenantPatch
            {
                ConfigurationVersion = configVersion.ToString(),
                PartiallyEnrolledTimestamp = DateTime.UtcNow,
                State = TenantState.PartiallyEnrolled.ToString(),
                Plan = plan.ToString()
            });
        }

        private async Task<List<DeploymentRing>> GetDeploymentRings(
            PlanType planType)
        {
            var deploymentRings = new List<DeploymentRing>();

            var updateRings = await this.tenantDataService.GetUpdateRingsAsync();

            // This is okay even for multi-plan calls since the DeploymentRings table will get updated on next run of the hourly function
            // Adding this filter so we do not attempt to look for deployment rings for more than the enrolling plan
            updateRings = updateRings
                .Where(updateRing => updateRing.GetPlanFromPolicy() == planType);

            foreach (var updateRing in updateRings)
            {
                var deploymentRing = new DeploymentRing
                {
                    Id = Guid.NewGuid(), // On Data Update, this will be replaced with the real one
                    FriendlyName = updateRing.FriendlyName,
                    FullName = updateRing.FullName,
                    UpdateChannel = updateRing.UpdateChannel,
                    QualityUpdatesDeferralPeriodInDays = updateRing.QualityUpdatesDeferralPeriodInDays,
                    FeatureUpdatesDeferralPeriodInDays = updateRing.FeatureUpdatesDeferralPeriodInDays,
                    AreQualityUpdatesPaused = false, // Paused defaults to false during enrollment
                    AreFeatureUpdatesPaused = false // Paused defaults to false during enrollment
                };

                deploymentRings.Add(deploymentRing);
            }

            return deploymentRings;
        }

        public async Task<IEnumerable<string>> CleanUpTenantOperations(Guid tenantId, ClaimsPrincipal identity)
        {
            using (var operationWrapper = new OperationWrapper(this.telemetryClient, nameof(CleanUpTenantOperations), tenantId.ToString(), string.Empty, string.Empty, new Dictionary<string, string>()))
            {
                ITenantShard shard = await this.tenantShardResolver.GetShardAsync(tenantId);
                var tenant = await shard.CustomerTenantRepository.FindTenantAsync();
                var operationCorrelationIds = new List<string>();

                await this.tenantManagementGateway.UnenrollAsync(tenant, identity);

                IOperationFactory operationFactory = await this.SetupMigration(tenant, identity, shard);
                var operations = await shard.CustomerTenantRepository.GetTenantOperationsAsync();
                List<IOperation> operationsToExecute = new List<IOperation>();

                foreach (OperationData od in operations)
                {
                    operationsToExecute.Add(operationFactory.CreateOperation(od));
                }

                operationsToExecute = operationsToExecute
                    .OrderByDescending(x => x.Precedence)
                    .ThenBy(x => x.Type == OperationType.QueueWin32App)
                    .ThenByDescending(x => x.ExecutionTime).ToList();

                foreach (OperationShell os in operationsToExecute)
                {
                    try
                    {
                        //try to revert the operation if revert is possible
                        await os.RevertOperation();
                        operationCorrelationIds.Add(os.CorrelationId.ToString());
                    }
                    catch (Exception ex)
                    {
                        operationWrapper.TrackTrace($"Operation does not have supported revert. Skipping. {ex.Message}");
                    }
                }

                operationWrapper.TrackTrace($"Operations have been reverted for test enrollment tenant: {tenantId}");

                //delete all tenant data
                await shard.CustomerTenantRepository.ResetTenantDataAsync();
                operationWrapper.TrackTrace($"All tenant data has been deleted for test enrollment tenant: {tenantId}");

                var deploymentRings = await shard.CustomerTenantRepository.GetTenantDeploymentRingsAsync();
                if (deploymentRings != null && deploymentRings.Any())
                {
                    await shard.CustomerTenantRepository.UpdateTenantConfigurationVersionAsync(0, DeployableRing.Test);
                    operationWrapper.TrackTrace($"Tenant configuration reset for test enrollment tenant: {tenantId}");
                }

                //return the operations which have been reverted successfully
                return operationCorrelationIds;
            }
        }

        public Task<EnrollmentConfiguration> GetLastKnownGoodConfigurationAsync()
        {
            return this.enrollmentConfigurationRepository.GetLastKnownGoodConfigurationAsync();
        }

        private async Task<string> AuthenticateToMsGraphAsync(
            ClaimsPrincipal identity)
        {
            Debug.Assert(identity != null);

            return await this.msGraphAuthenticationCallback(identity);
        }

        private async Task<string> AuthenticateToM365EventAuthoringAsync()
        {
            return await this.m365EventAuthoringAuthenticationCallback();
        }

        private async Task ValidateTenantCredentialStorageAsync(ITenantShard tenantShard, AdminTenant tenant)
        {
            var isTenantMigrated = await this.tenantCredentialResolver.IsTenantMigratedAsync(tenant);
            if (isTenantMigrated)
            {
                return;
            }

            var tenantKeyVault = await tenantShard.CustomerTenantRepository.FindKeyVaultAsync();
            if (tenantKeyVault is null)
            {
                throw new InvalidOperationException("The Tenant KeyVault has not been configured");
            }
        }

        /// <summary>
        /// Policy to collect device data and to configure Endpoiont Analytics
        /// </summary>
        /// <param name="tenant">the tenant whose devices data will be collected</param>
        /// <param name="assignJustMMDDevices">If true, assign to just MMD devices, else, assign to All Devices</param>
        /// <returns></returns>
        private async Task CreateWindowsHealthMonitoringConfiguration(CustomerTenant tenant, Func<bool, Task<string>> graphTokenDelegate, bool assignJustMMDDevices, PlanType plan, ITenantShard shard)
        {
            using (OperationWrapper operationWrapper = new OperationWrapper(this.telemetryClient, nameof(CreateWindowsHealthMonitoringConfiguration), tenant.DirectoryId.ToString(), tenant.Domain, string.Empty, new Dictionary<string, string>()))
            {
                // create Windows Health Monitoring Configuration Policy for Endpoint Analytics
                try
                {
                    await windowsHealthMonitoringService.CreateConfigurationPolicy(tenant, graphTokenDelegate);

                    if (assignJustMMDDevices)
                    {
                        await this.windowsHealthMonitoringService.AssignAllMMDDevicesGroups(tenant, graphTokenDelegate, plan);
                    }
                    else
                    {
                        await this.windowsHealthMonitoringService.AssignAllDevices(tenant, graphTokenDelegate);
                    }
                }
                catch (Exception e)
                {
                    var customProperties = new Dictionary<string, string>()
                    {
                        ["Error message"] = e.Message,
                        ["assignJustMMDDevices"] = assignJustMMDDevices.ToString()
                    };
                    operationWrapper.TrackEvent("Create Windows Health Monitoring Configuration Policy Failure", customProperties);
                    operationWrapper.TrackTrace("Enrollment: Create Windows Health Monitoring Configuration Policy Failure");
                    operationWrapper.TrackException(e);
                    await shard.CustomerTenantRepository.UpdateTenantStateAsync(TenantState.EnrollmentFailed, plan);
                    throw;
                }
            }
        }

        public async Task UpdateTenantWindowsDeviceLocationConsentAsync(
            CustomerTenant tenant,
            bool enableWindowsDeviceLocation)
        {
            Guard.NotNull(tenant, nameof(tenant));

            var eventProperties = new Dictionary<string, string>()
            {
                { "Tenant", tenant.Domain },
                { "tenantId", tenant.DirectoryId.ToString() },
                { "EnableWindowsDeviceLocation", enableWindowsDeviceLocation.ToString() }
            };

            var shard = await this.tenantShardResolver.GetShardAsync(tenant.DirectoryId);
            using (var operationWrapper = new OperationWrapper(this.telemetryClient, "UpdateTenantWindowsDeviceLocationConsent", tenant.DirectoryId.ToString(), tenant.Domain, string.Empty, eventProperties))
            {
                try
                {
                    await shard.CustomerTenantRepository.UpdateTenantWindowsDeviceLocationConsentAsync(enableWindowsDeviceLocation);
                }
                catch (Exception e)
                {
                    operationWrapper.TrackEvent("Update Tenant Windows Device Location Consent failed");
                    operationWrapper.TrackException(e);
                    throw;
                }
            }
        }

        public async Task UpdateTenantOperationsStatusAsync(CustomerTenant tenant, IEnumerable<OperationData> operations, OperationStatus operationStatusToBeUpdated)
        {
            Guard.NotNull(tenant, nameof(tenant));

            var eventProperties = new Dictionary<string, string>()
            {
                { "TenantId", tenant.DirectoryId.ToString() },
                { "TenantDomain", tenant.Domain }
            };

            using (var operationWrapper = new OperationWrapper(this.telemetryClient, "UpdateTenantOperationsStatusAsync", tenant.DirectoryId.ToString(), tenant.Domain, string.Empty, eventProperties))
            {
                var shard = await this.tenantShardResolver.GetShardAsync(tenant.DirectoryId);
                if (operations != null && operations.Any())
                {
                    await shard.CustomerTenantRepository.BulkUpdateTenantOperationAsync(operations, operationStatusToBeUpdated);
                }
            }
        }

        private Task<bool> IsModernConditionalAccessManagementFlightEnabledAsync(Guid tenantId)
        {
            return this.flightingResolver.IsFlightEnabled(Constants.ModernConditionalAccessManagementFlightEnabled, tenantId);
        }

        public async Task<Organization> GetOrganizationAsync(Guid tenantId, ClaimsPrincipal identity)
        {
            var accountService = accountServiceFactory.Create((useAppAuth) => this.AuthenticateToMsGraphAsync(identity));
            return await accountService.GetTenantOrganizationAsync(tenantId);
        }

        public async Task DeleteLocationServiceTenantMappingAsync(Guid tenantId, string scaleUnit)
        {
            var gateway = locationServiceGatewayFactory.CreatePartnerLocationServiceGateway();
            await gateway.DeleteLocationServiceTenantMappingAsync(tenantId, scaleUnit);
        }
    }
}
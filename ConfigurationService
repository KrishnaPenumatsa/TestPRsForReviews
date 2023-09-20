
using Microsoft.AzureAd.Icm.Types;
using Microsoft.AzureAd.Icm.WebService.Client;
using Microsoft.mWaaS.Common;
using Microsoft.mWaaS.Services.Core.Dal.Sql;
using Microsoft.mWaaS.Services.Core.Dal.Sql.Partners;
using Microsoft.mWaaS.Services.Core.Domain.Customers;
using Microsoft.mWaaS.Services.Core.Domain.Data;
using Microsoft.mWaaS.Services.Core.Domain.Partners;
using Microsoft.mWaaS.Services.Core.Domain.Services.AppControl;
using Microsoft.mWaaS.Services.Core.Domain.Services.Exceptions;
using Microsoft.mWaaS.Services.Core.Flighting;
using Microsoft.mWaaS.Services.Core.Operations;
using Microsoft.mWaaS.Services.Core.Utilities;
using Microsoft.mWaaS.Services.Graph;
using MMD.Services.Contracts.Common.Features;
using MMD.Services.Contracts.Common.Profile;
using MMD.Services.Contracts.Common.Tenant;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Net.Http;
using System.Security.Claims;
using System.Text;
using System.Threading.Tasks;
using static Microsoft.mWaaS.Services.Core.Domain.EnrollmentConfiguration;

namespace Microsoft.mWaaS.Services.Core.Domain.Services
{
    public sealed class ConfigurationService : IConfigurationService
    {
        private readonly IHttpClientFactory httpClientFactory;
        private readonly ISecurityBaselineRepository securityBaselineRepository;
        private readonly ITenantShardResolver tenantShardResolver;
        private readonly IEnrollmentService enrollmentService;
        private readonly IGroupServiceFactory groupServiceFactory;
        private readonly IUpdatesRepository updatesRepository;
        private readonly IAppLockerService appLockerService;
        private readonly ISqlProxyExceptionsRepository sqlProxyExceptionsRepository;
        private readonly IDeploymentTrackerService deploymentTrackerService;
        private readonly IMdmServiceFactory mdmServiceFactory;
        private readonly ITelemetryAdapter telemetryClient;
        private readonly Func<ClaimsPrincipal, Task<string>> msGraphAuthenticationCallback;
        private readonly IICMService icmService;
        private readonly IEmbeddedResourceReader embeddedResourceReader;
        private readonly IAppLockerBasePolicyRepository basePolicyRepository;
        private readonly IFlightingResolver flightingResolver;

        /// <summary>
        ///     Initializes a new instance of the <see cref="ConfigurationService"/>
        ///     class.
        /// </summary>
        public ConfigurationService(
            IHttpClientFactory httpClientFactory,
            ISecurityBaselineRepository securityBaselineRepository,
            ITenantShardResolver tenantShardResolver,
            IEnrollmentService enrollmentService,
            IGroupServiceFactory groupServiceFactory,
            IUpdatesRepository updatesRepository,
            IAppLockerService appLockerService,
            ISqlProxyExceptionsRepository sqlProxyExceptionsRepository,
            IDeploymentTrackerService deploymentTrackerService,
            IMdmServiceFactory mdmServiceFactory,
            ITelemetryAdapter telemetryClient,
            Func<ClaimsPrincipal, Task<string>> msGraphAuthenticationCallback,
            IICMService icmService,
            IEmbeddedResourceReader embeddedResourceReader,
            IAppLockerBasePolicyRepository basePolicyRepository,
            IFlightingResolver flightingResolver)
        {
            Guard.NotNull(httpClientFactory, nameof(httpClientFactory));
            Guard.NotNull(securityBaselineRepository, nameof(securityBaselineRepository));
            Guard.NotNull(tenantShardResolver, nameof(tenantShardResolver));
            Guard.NotNull(enrollmentService, nameof(enrollmentService));
            Guard.NotNull(groupServiceFactory, nameof(groupServiceFactory));
            Guard.NotNull(updatesRepository, nameof(updatesRepository));
            Guard.NotNull(appLockerService, nameof(appLockerService));
            Guard.NotNull(sqlProxyExceptionsRepository, nameof(sqlProxyExceptionsRepository));
            Guard.NotNull(deploymentTrackerService, nameof(deploymentTrackerService));
            Guard.NotNull(mdmServiceFactory, nameof(mdmServiceFactory));
            Guard.NotNull(telemetryClient, nameof(telemetryClient));
            Guard.NotNull(msGraphAuthenticationCallback, nameof(msGraphAuthenticationCallback));
            Guard.NotNull(icmService, nameof(icmService));
            Guard.NotNull(embeddedResourceReader, nameof(embeddedResourceReader));
            Guard.NotNull(basePolicyRepository, nameof(basePolicyRepository));
            Guard.NotNull(flightingResolver, nameof(flightingResolver));

            this.httpClientFactory = httpClientFactory;
            this.securityBaselineRepository = securityBaselineRepository;
            this.tenantShardResolver = tenantShardResolver;
            this.enrollmentService = enrollmentService;
            this.groupServiceFactory = groupServiceFactory;
            this.updatesRepository = updatesRepository;
            this.appLockerService = appLockerService;
            this.sqlProxyExceptionsRepository = sqlProxyExceptionsRepository;
            this.deploymentTrackerService = deploymentTrackerService;
            this.mdmServiceFactory = mdmServiceFactory;
            this.telemetryClient = telemetryClient;
            this.msGraphAuthenticationCallback = msGraphAuthenticationCallback;
            this.icmService = icmService;
            this.embeddedResourceReader = embeddedResourceReader;
            this.basePolicyRepository = basePolicyRepository;
            this.flightingResolver = flightingResolver;
        }

        public async Task SaveConfigurationSecurityBaseline(EnrollmentConfiguration ec, double version)
        {
            foreach (OperationData od in ec.PrototypeOperations.Where(x => x.Type == OperationType.CreateSyncMLSecurityBaseline))
            {
                var baseline = new SecurityBaseline()
                {
                    BaselineJson = od.Properties["baseline"],
                    Name = od.Properties["policyName"],
                    Description = od.Properties["policyName"],
                    Version = od.Properties.TryGetValue("osVersion", out string osVersion) ? osVersion : null,
                    Type = BaselineType.SyncML,
                    Platform = "Desktop",
                    OperatingSystem = "Windows",
                    From = "Redmond",
                    ConfigurationVersion = version

                };
                await this.securityBaselineRepository.SaveSecurityBaselineAsync(baseline, "system");
            }
        }

        public async Task<string> CheckOperationForBaselineConflict(string uri, AdminTenant tenant)
        {
            var shard = await this.tenantShardResolver.GetShardAsync(tenant.DirectoryId);
            var tenantConfiguration = await shard.CustomerTenantRepository.GetDeploymentRingConfigStatus();
            if (tenantConfiguration == null || tenantConfiguration.Test == null)
            {
                throw new InvalidOperationException("Tenant does not have a valid configuration.");
            }

            if (tenantConfiguration.Test.CurrentConfigurationVersion > 0)
            {
                List<SecurityBaseline> baselines = await this.securityBaselineRepository.GetSecurityBaselinesByConfigurationVersionAsync(tenantConfiguration.Test.CurrentConfigurationVersion);
                foreach (SecurityBaseline baseline in baselines)
                {
                    if (!String.IsNullOrEmpty(baseline.BaselineJson))
                    {
                        SyncML policies = JsonConvert.DeserializeObject<SyncML>(baseline.BaselineJson);
                        var conflicts = policies.SyncBody.Replace.Where(x => String.Equals(x.Item.Target.LocURI, uri));
                        if (conflicts.Any())
                        {
                            return baseline.Name;
                        }
                    }
                }
            }
            return null;
        }

        public async Task<CustomerConfigurableSetting> CreateBackgroundSetting(CustomerTenant tenant, ClaimsPrincipal identity, string backgroundUrl, Func<bool, Task<string>> graphTokenDelegate = null)
        {
            var shard = await this.tenantShardResolver.GetShardAsync(tenant.DirectoryId);

            int version = await shard.CustomerTenantRepository.GetLatestVersionCustomerConfigurableByType(CustomerConfigurableSettingType.Background);
            version++;
            Dictionary<string, string> eventProperties = new Dictionary<string, string>()
                    {
                        { "Version", version.ToString() },
                        { "Tenant", tenant.Domain },
                        { "TenantId", tenant.DirectoryId.ToString() }
                    };

            using (OperationWrapper ow = new OperationWrapper(this.telemetryClient, "CreateBackgroundSetting", tenant.DirectoryId.ToString(), tenant.Domain, string.Empty, eventProperties))
            {
                if (tenant.State == TenantState.Enrolled || tenant.State == TenantState.PartiallyEnrolled)
                {
                    string displayName = "Customer Configurable - Desktop Background v" + version.ToString();
                    OperationData od = new OperationData()
                    {
                        Type = OperationType.CreateCustomPolicy,
                        Category = OperationCategory.CustomerConfigurable,
                        Properties = new Dictionary<string, string>
                    {
                        { "DisplayName", displayName },
                        { "Description",  "Desktop Background settings configured by the customer" },
                        { "DataType", "chr" },
                        { "OmaUri", "./Vendor/MSFT/Personalization/DesktopImageUrl" },
                        { "Value", backgroundUrl }
                    },
                        TenantId = tenant.DirectoryId,
                        CorrelationId = Guid.NewGuid()
                    };

                    Dictionary<string, string> properties = new Dictionary<string, string>()
                {
                    { "personalizationDesktopImageUrl", backgroundUrl },
                };

                    return await ExecuteCreateCustomerConfigurableSetting(od, tenant, shard, identity, graphTokenDelegate, version, displayName, CustomerConfigurableSettingType.Background, properties);

                }
            }
            return null;
        }

        public async Task<CustomerConfigurableSetting> CreateStartPagesSetting(CustomerTenant tenant, ClaimsPrincipal identity, string startPageUrls, Func<bool, Task<string>> graphTokenDelegate = null)
        {
            var shard = await this.tenantShardResolver.GetShardAsync(tenant.DirectoryId);

            int version = await shard.CustomerTenantRepository.GetLatestVersionCustomerConfigurableByType(CustomerConfigurableSettingType.StartPages);
            version++;
            Dictionary<string, string> eventProperties = new Dictionary<string, string>()
                    {
                        { "Version", version.ToString() },
                        { "Tenant", tenant.Domain },
                        { "TenantId", tenant.DirectoryId.ToString() }
                    };
            using (OperationWrapper ow = new OperationWrapper(this.telemetryClient, "CreateStartPagesSetting", tenant.DirectoryId.ToString(), tenant.Domain, string.Empty, eventProperties))
            {
                if (tenant.State == TenantState.Enrolled || tenant.State == TenantState.PartiallyEnrolled)
                {
                    string displayName = "Customer Configurable - Start Pages v" + version.ToString();
                    OperationData od = new OperationData()
                    {
                        Type = OperationType.CreateCustomPolicy,
                        Category = OperationCategory.CustomerConfigurable,
                        Properties = new Dictionary<string, string>
                        {
                            { "DisplayName", displayName },
                            { "Description",  "Start Pages Setting configured by the customer" },
                            { "DataType", "chr" },
                            { "OmaUri", "./Vendor/MSFT/Policy/Config/Browser/HomePages" },
                            { "Value", startPageUrls }
                        },
                        TenantId = tenant.DirectoryId,
                        CorrelationId = Guid.NewGuid()
                    };

                    Dictionary<string, string> properties = new Dictionary<string, string>()
                    {
                        { "edgeHomepageUrls", startPageUrls }
                    };

                    return await ExecuteCreateCustomerConfigurableSetting(od, tenant, shard, identity, graphTokenDelegate, version, displayName, CustomerConfigurableSettingType.StartPages, properties);

                }
            }
            return null;
        }

        public async Task<ProxyExceptions> CreateMMDProxyExceptionsRecord(string mmdProxyExceptions)
        {
            await this.sqlProxyExceptionsRepository.CreateMMDProxyExceptions(mmdProxyExceptions);
            return await this.sqlProxyExceptionsRepository.GetLatestMMDProxyExceptions();
        }

        public async Task<CustomerConfigurableSetting> CreateProxySetting(
            CustomerTenant tenant,
            ClaimsPrincipal identity,
            string serverAddress,
            string portNumber,
            string proxyExceptions,
            Func<bool, Task<string>> graphTokenDelegate = null)
        {
            var shard = await this.tenantShardResolver.GetShardAsync(tenant.DirectoryId);

            int version = await shard.CustomerTenantRepository.GetLatestVersionCustomerConfigurableByType(CustomerConfigurableSettingType.Proxy);
            ProxyExceptions mmdProxyExceptions = await this.sqlProxyExceptionsRepository.GetLatestMMDProxyExceptions();
            if (mmdProxyExceptions == null)
            {
                throw new InvalidOperationException("No MMD proxy exclusion list exists");
            }

            if (mmdProxyExceptions != null && !String.IsNullOrEmpty(proxyExceptions) && !String.IsNullOrEmpty(mmdProxyExceptions.Exceptions))
            {
                mmdProxyExceptions.Exceptions = String.Concat(mmdProxyExceptions.Exceptions, ';');
            }
            version++;
            Dictionary<string, string> eventProperties = new Dictionary<string, string>()
                    {
                        { "Version", version.ToString() },
                        { "Tenant", tenant.Domain },
                        { "TenantId", tenant.DirectoryId.ToString() }
                    };
            using (OperationWrapper ow = new OperationWrapper(this.telemetryClient, "CreateProxySetting", tenant.DirectoryId.ToString(), tenant.Domain, string.Empty, eventProperties))
            {
                var aggregatedExceptions = mmdProxyExceptions.Exceptions + proxyExceptions;
                if (aggregatedExceptions.Length > 2064)
                {
                    AlertSourceIncident incidentToSend;

                    ICMSubmittedTicket submissionTicket = new ICMSubmittedTicket()
                    {
                        Category = "Customer Unique Configuration",
                        Description = $"Proxy exception list of {aggregatedExceptions} exceeds max lenght of 2064. Length is {aggregatedExceptions.Length.ToString()}. {tenant.Domain}",
                        Severity = 4,
                        Type = "Change Request",
                        Title = $"Proxy Exception List Exceeds Max Length: {tenant.Domain}"
                    };
                    ICMTicket ticket = new ICMTicket(submissionTicket);
                    ticket.tenantID = tenant.DirectoryId;
                    ticket.Environment = "Ibiza Portal";
                    ticket.CustomerName = tenant.Domain;

                    // Fetch a new incident to send
                    incidentToSend = this.icmService.CreateIncidentToSend(ticket, "Proxy Exception List Too Long", IncidentFiler.Service);
                    using (ConnectorIncidentManagerClient ws = this.icmService.CreateConnectorClient())
                    {
                        IncidentAddUpdateResult result;

                        // Attempt to submit the incident to IcM
                        result = ws.AddOrUpdateIncident2(this.icmService.ICMGuid, incidentToSend, RoutingOptions.None);
                        throw new ProxyExceptionsExceedMaxLengthException($"Proxy Exceptions exceed max length. A ticket ({result.IncidentId}) has been filed for this incident and MMD Operations is investigating.");

                    }
                }

                if (tenant.State == TenantState.Enrolled || tenant.State == TenantState.PartiallyEnrolled)
                {
                    string name = "Customer Configurable - Proxy v" + version.ToString();
                    OmaSetting[] settings =
                    {
                        new OmaSettingString(){
                            DisplayName = name,
                            Description = "Hostname and port of the proxy server",
                            OmaUri = "./Vendor/MSFT/NetworkProxy/ProxyServer/ProxyAddress",
                            Value = serverAddress + ":" + portNumber
                        },
                        new OmaSettingString(){
                            DisplayName = name,
                            Description = "URLs in this list are not sent to the proxy server",
                            OmaUri = "./Vendor/MSFT/NetworkProxy/ProxyServer/Exceptions",
                            Value = mmdProxyExceptions.Exceptions + proxyExceptions
                        },
                        new OmaSettingInteger()
                        {
                            DisplayName = name,
                            Description = "Does not allow the user to edit the proxy",
                            OmaUri = "./Vendor/MSFT/NetworkProxy/ProxySettingsPerUser",
                            Value = 0
                        },
                        new OmaSettingString()
                        {
                            DisplayName = name,
                            Description = "Lock down the proxy settings for standard users",
                            OmaUri = "./Device/Vendor/MSFT/Policy/Config/InternetExplorer/DisableProxyChange",
                            Value = "<Enabled/>"
                        },
                        new OmaSettingInteger()
                        {
                            DisplayName = name,
                            Description = "Don't use proxy for local addresses",
                            OmaUri = "./Vendor/MSFT/NetworkProxy/UseProxyForLocalAddresses",
                            Value = 1
                        }
                    };

                    OmaUriGreySetting proxySetting = new OmaUriGreySetting()
                    {
                        displayName = name,
                        omaSettings = settings
                    };

                    OperationData od = new OperationData
                    {
                        Type = OperationType.CreateIntuneSpecificPolicy,
                        Category = OperationCategory.CustomerConfigurable,
                        Properties = new Dictionary<string, string>
                        {
                            { "DisplayName", name },
                            { "PolicyContent", JsonConvert.SerializeObject(proxySetting) }
                        },
                        TenantId = tenant.DirectoryId,
                        CorrelationId = Guid.NewGuid()
                    };

                    Dictionary<string, string> properties = new Dictionary<string, string>()
                    {
                        { "serverAddress", serverAddress },
                        { "portNumber", portNumber },
                        { "proxyExceptions", proxyExceptions }
                    };
                    return await ExecuteCreateCustomerConfigurableSetting(od, tenant, shard, identity, graphTokenDelegate, version, name, CustomerConfigurableSettingType.Proxy, properties);
                }
            }
            return null;
        }

        public async Task<CustomerConfigurableSetting> CreateTrustedSitesSetting(
            CustomerTenant tenant,
            ClaimsPrincipal identity,
            string trustedSites,
            Func<bool, Task<string>> graphTokenDelegate = null)
        {
            var shard = await this.tenantShardResolver.GetShardAsync(tenant.DirectoryId);

            int version = await shard.CustomerTenantRepository.GetLatestVersionCustomerConfigurableByType(CustomerConfigurableSettingType.TrustedSites);
            version++;
            Dictionary<string, string> eventProperties = new Dictionary<string, string>()
                    {
                        { "Version", version.ToString() },
                        { "Tenant", tenant.Domain },
                        { "TenantId", tenant.DirectoryId.ToString() }
                    };
            using (OperationWrapper ow = new OperationWrapper(this.telemetryClient, "CreateTrustedSitesSetting", tenant.DirectoryId.ToString(), tenant.Domain, string.Empty, eventProperties))
            {
                if (tenant.State == TenantState.Enrolled || tenant.State == TenantState.PartiallyEnrolled)
                {
                    string displayName = "Customer Configurable - Trusted Sites v" + version.ToString();
                    OperationData od = new OperationData()
                    {
                        Type = OperationType.CreateCustomPolicy,
                        Category = OperationCategory.CustomerConfigurable,
                        Properties = new Dictionary<string, string>
                        {
                            { "DisplayName", displayName },
                            { "Description",  "List of trusted sites and zones" },
                            { "DataType", "Chr" },
                            { "OmaUri", "./Device/Vendor/MSFT/Policy/Config/InternetExplorer/AllowSiteToZoneAssignmentList" },
                            { "Value", trustedSites }
                        },
                        TenantId = tenant.DirectoryId,
                        CorrelationId = Guid.NewGuid()
                    };

                    Dictionary<string, string> properties = new Dictionary<string, string>()
                    {
                        { "trustedSites", trustedSites }
                    };
                    return await ExecuteCreateCustomerConfigurableSetting(od, tenant, shard, identity, graphTokenDelegate, version, displayName, CustomerConfigurableSettingType.TrustedSites, properties);
                }
            }
            return null;
        }

        public async Task<CustomerConfigurableSetting> CreateEnterpriseModeSitesSetting(
            CustomerTenant tenant,
            ClaimsPrincipal identity,
            string enterpriseModeListLocation,
            Func<bool, Task<string>> graphTokenDelegate = null)
        {
            var shard = await this.tenantShardResolver.GetShardAsync(tenant.DirectoryId);

            int version = await shard.CustomerTenantRepository.GetLatestVersionCustomerConfigurableByType(CustomerConfigurableSettingType.EnterpriseModeSites);
            version++;
            Dictionary<string, string> eventProperties = new Dictionary<string, string>()
                    {
                        { "Version", version.ToString() },
                        { "Tenant", tenant.Domain },
                        { "TenantId", tenant.DirectoryId.ToString() }
                    };
            using (OperationWrapper ow = new OperationWrapper(this.telemetryClient, "CreateEnterpriseModeSitesSetting", tenant.DirectoryId.ToString(), tenant.Domain, string.Empty, eventProperties))
            {
                if (tenant.State == TenantState.Enrolled || tenant.State == TenantState.PartiallyEnrolled)
                {
                    string displayName = "Customer Configurable - Enterprise Mode Sites v" + version.ToString();
                    OmaSetting[] settings =
                    {
                        new OmaSettingString(){
                            DisplayName = displayName,
                            Description = "Browser policy for list of websites to be opened in enterprise mode",
                            OmaUri = "./Device/Vendor/MSFT/Policy/Config/Browser/EnterpriseModeSiteList",
                            Value = enterpriseModeListLocation
                        },
                        new OmaSettingString(){
                            DisplayName = displayName,
                            Description = "IE policy for list of websites to be opened in enterprise mode",
                            OmaUri = "./Device/Vendor/MSFT/Policy/Config/InternetExplorer/AllowEnterpriseModeSiteList",
                            Value = "<enabled/><data id=\"EnterSiteListPrompt\" value=\"" + enterpriseModeListLocation + "\"/><data id=\"EnterReportBackPrompt\" value=\"Enable\"/>"
                        }
                    };

                    OmaUriGreySetting enterpriseModeSetting = new OmaUriGreySetting()
                    {
                        displayName = displayName,
                        omaSettings = settings
                    };

                    OperationData od = new OperationData()
                    {
                        Type = OperationType.CreateIntuneSpecificPolicy,
                        Category = OperationCategory.CustomerConfigurable,
                        Properties = new Dictionary<string, string>
                        {
                            { "DisplayName", displayName },
                            { "PolicyContent", JsonConvert.SerializeObject(enterpriseModeSetting) }
                        },
                        TenantId = tenant.DirectoryId,
                        CorrelationId = Guid.NewGuid()
                    };

                    Dictionary<string, string> properties = new Dictionary<string, string>()
                    {
                        { "enterpriseModeListLocation", enterpriseModeListLocation }
                    };
                    return await ExecuteCreateCustomerConfigurableSetting(od, tenant, shard, identity, graphTokenDelegate, version, displayName, CustomerConfigurableSettingType.EnterpriseModeSites, properties);
                }
            }
            return null;
        }

        public async Task<CustomerConfigurableSetting> CreateAppLockerSetting(CustomerTenant tenant, ClaimsPrincipal identity, bool auditMode, Func<bool, Task<string>> graphTokenDelegate = null)
        {
            var shard = await this.tenantShardResolver.GetShardAsync(tenant.DirectoryId);
            Dictionary<string, string> eventProperties = new Dictionary<string, string>()
                    {
                        { "Tenant", tenant.Domain },
                        { "TenantId", tenant.DirectoryId.ToString() }
                    };
            using (OperationWrapper ow = new OperationWrapper(this.telemetryClient, "CreateAppLockerSetting", tenant.DirectoryId.ToString(), tenant.Domain, string.Empty, eventProperties))
            {
                if (tenant.State == TenantState.Enrolled || tenant.State == TenantState.PartiallyEnrolled)
                {
                    // AppLockerVersion is in the format BaseVersion.CustomVersion
                    var versionParts = tenant.AppLockerVersion.Split('.');

                    // Get the most current base version of the policy
                    var currentBaseVersion = await basePolicyRepository.GetCurrentBaseConfigVersion(Persona.All);

                    // Since we are deploying a new version of AppLocker, we need to increment the custom version
                    var newCustomVersion = Int32.Parse(versionParts[1]) + 1;

                    var newFullVersion = string.Format(CultureInfo.InvariantCulture, "{0}.{1}", currentBaseVersion, newCustomVersion);
                    // Save new version in tenant
                    await shard.CustomerTenantRepository.UpdateTenantAppLockerVersionAsync(newFullVersion);

                    var featureFlags = await shard.CustomerTenantRepository.GetTenantFeatureFlags();
                    featureFlags.TryGetValue(nameof(Features.DisableAppLockerDllRules), out bool disableDllRules);

                    var appLockerConfiguration = await appLockerService.GetAppLockerConfiguration(tenant.DirectoryId, auditMode, currentBaseVersion, newCustomVersion, disableDllRules);
                    OperationData od = new OperationData
                    {
                        Type = OperationType.CreateIntuneSpecificPolicy,
                        Category = OperationCategory.CustomerConfigurable,
                        Properties = new Dictionary<string, string>
                        {
                            { "DisplayName", appLockerConfiguration.configuration.displayName },
                            { "PolicyContent", JsonConvert.SerializeObject(appLockerConfiguration.configuration) }
                        },
                        TenantId = tenant.DirectoryId,
                        CorrelationId = Guid.NewGuid()
                    };
                    return await ExecuteCreateCustomerConfigurableSetting(od, tenant, shard, identity, graphTokenDelegate, appLockerConfiguration.baseVersion,
                        appLockerConfiguration.configuration.displayName, CustomerConfigurableSettingType.AppLocker, new Dictionary<string, string>());
                }
            }
            return null;
        }

        public async Task<CustomerConfigurableSetting> RollbackCustomerConfigurableSetting(CustomerTenant tenant, ClaimsPrincipal identity, int id, RingRollout ring, Func<bool, Task<string>> graphTokenDelegate = null)
        {
            var shard = await this.tenantShardResolver.GetShardAsync(tenant.DirectoryId);

            CustomerConfigurableSetting setting = await shard.CustomerTenantRepository.GetCustomerConfigurableSettingById(id);
            Dictionary<string, string> eventProperties = new Dictionary<string, string>()
                    {
                        { "Tenant", tenant.Domain },
                        { "TenantId", tenant.DirectoryId.ToString() },
                        { "ConfigurableSettingId", id.ToString()},
                        { "Ring", ring.ToString()},
                        { "Type", setting.Type.ToString()},
                        { "Version", setting.Version.ToString()}
                    };
            using (OperationWrapper ow = new OperationWrapper(this.telemetryClient, "RollbackCustomerConfigurableSetting", tenant.DirectoryId.ToString(), tenant.Domain, string.Empty, eventProperties))
            {
                if (setting != null && (setting.RolloutStatus & ring) != 0)
                {
                    // Need to get operation which assigned the policy to the group where category = customerconfigurable
                    IEnumerable<OperationData> operations = await shard.CustomerTenantRepository.GetCustomerConfigurableOperations();

                    // Then need to find where type = assigntogroup and properties has policies = setting.PolicyName and groupname = "modern workplace devices-" + ring
                    operations = operations.Where(x => x.Status == OperationStatus.Complete &&
                        x.Type == OperationType.AssignPoliciesToGroup &&
                        x.Properties.ContainsValue(setting.PolicyName) &&
                        x.Properties.ContainsValue(UtilityMethods.GetDeviceGroupFromRing(ring)));
                    if (operations.Count() == 0)
                    {
                        return null;
                    }

                    // There should only be one operation, but if that's not the case, the result is the same from just reverting the first one.
                    OperationData revert = new OperationData
                    {
                        Type = OperationType.RevertOperation,
                        Category = OperationCategory.CustomerConfigurable,
                        Properties = new Dictionary<string, string>
                        {
                            { "CorrelationIdToRevert", operations.First().CorrelationId.ToString() },
                        },
                        TenantId = tenant.DirectoryId,
                        CorrelationId = Guid.NewGuid()
                    };
                    revert.Id = await shard.CustomerTenantRepository.InsertTenantOperationAsync(revert);
                    revert = await this.enrollmentService.MigrateSingleOperationAsync(tenant, identity, revert, graphTokenDelegate);
                    return await shard.CustomerTenantRepository.UpdateRolloutStatusCustomerConfigurableSettingAsync(id, setting.RolloutStatus - (int)ring);
                }
            }
            return null;
        }

        public async Task<CustomerConfigurableSetting> RolloutCustomerConfigurableSetting(
            CustomerTenant tenant,
            ClaimsPrincipal identity,
            int id,
            RingRollout ring,
            Func<bool, Task<string>> graphTokenDelegate = null,
            Func<DeploymentTrackerInput, Task<DeploymentTracker>> createTracker = null)
        {
            var shard = await this.tenantShardResolver.GetShardAsync(tenant.DirectoryId);
            CustomerConfigurableSetting setting = await shard.CustomerTenantRepository.GetCustomerConfigurableSettingById(id);
            Dictionary<string, string> eventProperties = new Dictionary<string, string>()
                    {
                        { "Tenant", tenant.Domain },
                        { "TenantId", tenant.DirectoryId.ToString() },
                        { "ConfigurableSettingId", id.ToString()},
                        { "Ring", ring.ToString()},
                        { "Type", setting.Type.ToString()},
                        { "Version", setting.Version.ToString()}
                    };
            using (OperationWrapper ow = new OperationWrapper(this.telemetryClient, "RolloutCustomerConfigurableSetting", tenant.DirectoryId.ToString(), tenant.Domain, string.Empty, eventProperties))
            {
                if (setting != null &&
                (setting.RolloutStatus & ring) == 0 &&
                setting.Status != CustomerConfigurableSettingStatus.Aborted &&
                setting.Status != CustomerConfigurableSettingStatus.Cancelled)
                {
                    OperationData od = new OperationData
                    {
                        Type = OperationType.AssignPoliciesToGroup,
                        Category = OperationCategory.CustomerConfigurable,
                        Properties = new Dictionary<string, string>
                        {
                            { "GroupName", UtilityMethods.GetDeviceGroupFromRing(ring) },
                            { "Policies", setting.PolicyName }
                        },
                        TenantId = tenant.DirectoryId,
                        CorrelationId = Guid.NewGuid()
                    };
                    od.Id = await shard.CustomerTenantRepository.InsertTenantOperationAsync(od);
                    od = await this.enrollmentService.MigrateSingleOperationAsync(tenant, identity, od, graphTokenDelegate);

                    // Find any other customer configurable settings in DB which are currently rolled out to ring and revert
                    IEnumerable<CustomerConfigurableSettingDeploymentTracker> settings = await shard.CustomerTenantRepository.GetCustomerConfigurableSettingsByType(setting.Type);
                    settings = settings.Where(x => (x.RolloutStatus & ring) == ring && x.Id != id);
                    //There should only be one max rolled out that isn't the current one. However, it would make sense to rollback all if more than one exists.
                    foreach (CustomerConfigurableSettingDeploymentTracker ccs in settings)
                    {
                        await this.RollbackCustomerConfigurableSetting(tenant, identity, ccs.Id, ring, graphTokenDelegate);
                    }

                    if (graphTokenDelegate.IsNull())
                    {
                        graphTokenDelegate = (useAppAuth) => this.AuthenticateToMsGraphAsync(identity);
                    }

                    DeploymentTracker dT = await InitiateDeploymentTracker(setting.PolicyName, tenant.DirectoryId, ring, graphTokenDelegate, id, createTracker, ow);

                    if (!dT.IsNull())
                    {
                        setting = await shard.CustomerTenantRepository.UpdateRolloutStatusCustomerConfigurableSettingAsync(id, setting.RolloutStatus | ring, setting.RolloutHistory | ring, ring, dT.Id);
                    }
                    else
                    {
                        setting = await shard.CustomerTenantRepository.UpdateRolloutStatusCustomerConfigurableSettingAsync(id, setting.RolloutStatus | ring, setting.RolloutHistory | ring, ring);
                    }

                    return setting;
                }
            }
            return null;
        }

        private async Task<DeploymentTracker> InitiateDeploymentTracker(
            string entityName,
            Guid tenantId,
            RingRollout ring,
            Func<bool, Task<string>> graphTokenDelegate,
            int correlationId,
            Func<DeploymentTrackerInput, Task<DeploymentTracker>> createTracker,
            OperationWrapper ow)
        {
            DeploymentTracker dT = null;

            ow.TrackTrace($"Attempting to create a Customer Configurable Setting deployment tracker for Tenant - {tenantId}, Ring - {ring.ToString()}, Config Id - {correlationId.ToString()}");
            try
            {

                var entity = new DeploymentTrackerEntity() { EntityName = entityName, EntityType = EntityType.Policy };
                var entities = new List<DeploymentTrackerEntity>();
                entities.Add(entity);

                Guard.NotNull(createTracker, nameof(createTracker));

                dT = await createTracker(
                    new DeploymentTrackerInput
                    {
                        TenantId = tenantId,
                        Source = DeploymentSource.GreySettings,
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
                ow.TrackTrace($"Unable to create deployment tracker for Customer Configurable Setting. Tenant - {tenantId}, Ring - {ring.ToString()}, Config Id - {correlationId.ToString()}");
                ow.TrackException(e);
            }

            return dT;
        }

        public async Task<CustomerConfigurableSetting> AbortCustomerConfigurableSetting(CustomerTenant tenant, ClaimsPrincipal identity, int id, Func<bool, Task<string>> graphTokenDelegate = null)
        {
            var shard = await this.tenantShardResolver.GetShardAsync(tenant.DirectoryId);
            CustomerConfigurableSetting setting = await shard.CustomerTenantRepository.GetCustomerConfigurableSettingById(id);
            IEnumerable<CustomerConfigurableSettingDeploymentTracker> settings = await shard.CustomerTenantRepository.GetCustomerConfigurableSettingsByType(setting.Type);
            await shard.CustomerTenantRepository.AbortCustomerConfigurableSetting(id);

            Dictionary<string, string> eventProperties = new Dictionary<string, string>()
            {
                { "Tenant", tenant.Domain },
                { "TenantId", tenant.DirectoryId.ToString() },
                { "ConfigurableSettingId", id.ToString()},
                { "Type", setting.Type.ToString()},
                { "Version", setting.Version.ToString()}
            };
            using (OperationWrapper ow = new OperationWrapper(this.telemetryClient, "AbortCustomerConfigurableSetting", tenant.DirectoryId.ToString(), tenant.Domain, string.Empty, eventProperties))
            {

                // Find LKG, rollout all rings of the lkg which will also rollback all other rings for the current setting
                var lkg = settings.Where(x => x.IsKnownGood == true && x.Id != id).OrderByDescending(y => y.Version).FirstOrDefault();
                RingRollout[] rings = { RingRollout.Test, RingRollout.First, RingRollout.Fast, RingRollout.Broad };
                if (!lkg.IsNull())
                {
                    foreach (RingRollout ring in rings)
                    {
                        await this.RolloutCustomerConfigurableSetting(tenant, identity, lkg.Id, ring, graphTokenDelegate);
                    }
                }
                else
                {
                    foreach (RingRollout ring in rings)
                    {
                        await this.RollbackCustomerConfigurableSetting(tenant, identity, id, ring, graphTokenDelegate);
                    }
                }
                return await shard.CustomerTenantRepository.GetCustomerConfigurableSettingById(id);
            }
        }

        public async Task<CustomerConfigurableSetting> CancelCustomerConfigurableSetting(CustomerTenant tenant, ClaimsPrincipal identity, int id, Func<bool, Task<string>> graphTokenDelegate = null)
        {
            var shard = await this.tenantShardResolver.GetShardAsync(tenant.DirectoryId);
            CustomerConfigurableSetting setting = await shard.CustomerTenantRepository.GetCustomerConfigurableSettingById(id);
            Dictionary<string, string> eventProperties = new Dictionary<string, string>()
            {
                { "Tenant", tenant.Domain },
                { "TenantId", tenant.DirectoryId.ToString() },
                { "ConfigurableSettingId", id.ToString()},
                { "Type", setting.Type.ToString()},
                { "Version", setting.Version.ToString()}
            };
            using (OperationWrapper ow = new OperationWrapper(this.telemetryClient, "CancelCustomerConfigurableSetting", tenant.DirectoryId.ToString(), tenant.Domain, string.Empty, eventProperties))
            {
                RingRollout[] rings = { RingRollout.Test, RingRollout.First, RingRollout.Fast, RingRollout.Broad };
                foreach (RingRollout ring in rings)
                {
                    await this.RollbackCustomerConfigurableSetting(tenant, identity, id, ring, graphTokenDelegate);
                }
                telemetryClient.TrackTrace("Cancelling customer configurable setting with id = " + id.ToString());
                return await shard.CustomerTenantRepository.CancelCustomerConfigurableSetting(id);
            }
        }

        public async Task<IEnumerable<DeployableRing>> GetRingsToDeploy(AdminTenant tenant, double version, DeployableRing ringToDeploy)
        {
            using (var ow = new OperationWrapper(this.telemetryClient, nameof(GetRingsToDeploy), tenant.DirectoryId.ToString(), tenant.Domain, tenant.DirectoryId.ToString(), null))
            {
                try
                {
                    var shard = await this.tenantShardResolver.GetShardAsync(tenant.DirectoryId);
                    var deploymentStatus = await shard.CustomerTenantRepository.GetDeploymentRingConfigStatus();
                    var currentConfigVersions = new Dictionary<DeployableRing, double>
                    {
                        { DeployableRing.Test, deploymentStatus.Test.CurrentConfigurationVersion },
                        { DeployableRing.First, deploymentStatus.First.CurrentConfigurationVersion },
                        { DeployableRing.Fast, deploymentStatus.Fast.CurrentConfigurationVersion },
                        { DeployableRing.Broad, deploymentStatus.Broad.CurrentConfigurationVersion },
                    };

                    return currentConfigVersions.Where(x => version > x.Value && (int)ringToDeploy >= (int)x.Key).Select(x => x.Key);
                }
                catch (Exception e)
                {
                    ow.TrackException(e);
                    throw;
                }
            }
        }

        private void GetRingVersions(double targetVersion, double broadVersion, DeployableRing targetRing, List<DeployableRingVersion> ringVersions)
        {
            //                  Test    First   Fast    Broad
            // Current Version:   1       1       1       1
            // Target Version:    2       2       1       1
            //                  Test    First   Fast    Broad
            // Current Version:   2       1       1       1     (Test would finish before First)
            // Target Version:    2       2       1       1

            if (targetVersion != broadVersion)
            {
                ringVersions.Add(new DeployableRingVersion
                {
                    ring = targetRing,
                    currentVersion = broadVersion,
                    targetVersion = targetVersion
                });
            }
        }

        private void GetRingToAdd(RingConfigurationStatus ringStatus, DeployableRing ring, Dictionary<RingConfigurationStatus, DeployableRing> ringsToAdd)
        {
            if (ringStatus.TargetConfigurationVersion != 0)
            {
                ringsToAdd.Add(ringStatus, ring);
            }
        }

        public async Task<IEnumerable<DeployableRingVersion>> GetRingsToAlignPlan(AdminTenant tenant)
        {
            using (var ow = new OperationWrapper(this.telemetryClient, nameof(GetRingsToAlignPlan), tenant.DirectoryId.ToString(), tenant.Domain, tenant.DirectoryId.ToString(), null))
            {
                try
                {
                    var shard = await this.tenantShardResolver.GetShardAsync(tenant.DirectoryId);
                    var deploymentStatus = await shard.CustomerTenantRepository.GetDeploymentRingConfigStatus();

                    var ringsToAdd = new Dictionary<RingConfigurationStatus, DeployableRing>();
                    GetRingToAdd(deploymentStatus.Test, DeployableRing.Test, ringsToAdd);
                    GetRingToAdd(deploymentStatus.First, DeployableRing.First, ringsToAdd);
                    GetRingToAdd(deploymentStatus.Fast, DeployableRing.Fast, ringsToAdd);
                    GetRingToAdd(deploymentStatus.Broad, DeployableRing.Broad, ringsToAdd);

                    var ringVersions = new List<DeployableRingVersion>();
                    foreach (var ringConf in ringsToAdd)
                    {
                        GetRingVersions(ringConf.Key.TargetConfigurationVersion, deploymentStatus.Broad.CurrentConfigurationVersion, ringConf.Value, ringVersions);
                    }

                    if (ringVersions.Count() == 0)
                    {
                        // In plan Premium:
                        // CASE1:
                        //           Previous Status(Qian's process API, not this API)                      Now This API is called
                        //                  Test    First   Fast    Broad                                Test    First   Fast    Broad
                        // Current Version:   2       2       2       1         =>      Current Version:   2       2       2       2
                        // Target Version:    2       2       2       1                 Target Version:    2       2       2       2
                        // CASE2:
                        //                  Test    First   Fast    Broad                                Test    First   Fast    Broad
                        // Current Version:   2       2       2       2         =>      Current Version:   2       2       2       2 
                        // Target Version:    2       2       2       2                 Target Version:    2       2       2       2
                        // 
                        //
                        // In these 2 cases, ringVersion is empty.
                        // CASE1: New plan will be updated to 1 1 1 1 in previous process (aka. Qian's API)
                        //                                    1 1 1 1
                        // And we need to updated to 2 2 2 2
                        // CASE2: New plan will be updated to 2 2 2 2 in previous process (aka. Qian's API)
                        //                                    2 2 2 2
                        // We do not need to update anything, and the status would be 2 2 2 2
                        //
                        // So we add these 4 rings to get compatible with these 2 cases, which means targetVersion are all 2.
                        // In addition, we know targetVersion is 2, but currentVersion is unknown in this new plan, so we set current Version to -1.
                        foreach (var ringConf in ringsToAdd)
                        {
                            ringVersions.Add(new DeployableRingVersion
                            {
                                ring = ringConf.Value,
                                currentVersion = -1, // current version unknown
                                targetVersion = ringConf.Key.TargetConfigurationVersion
                            });
                        }
                    }

                    return ringVersions;
                }
                catch (Exception e)
                {
                    ow.TrackException(e);
                    throw;
                }
            }
        }

        public async Task<string> ExportConfigOperationsAsPowerShellScripts(CustomerTenant tenant, ClaimsPrincipal identity)
        {
            var shard = await this.tenantShardResolver.GetShardAsync(tenant.DirectoryId);
            int version = await shard.CustomerTenantRepository.GetLatestVersionCustomerConfigurableByType(CustomerConfigurableSettingType.Background);
            version++;

            Dictionary<string, string> eventProperties = new Dictionary<string, string>()
                    {
                        { "Version", version.ToString() },
                        { "Tenant", tenant.Domain },
                        { "TenantId", tenant.DirectoryId.ToString() }
                    };

            using (OperationWrapper ow = new OperationWrapper(this.telemetryClient, "ExportConfigOperationsAsPowerShellScripts", tenant.DirectoryId.ToString(), tenant.Domain, string.Empty, eventProperties))
            {

                IGroupService groupService = this.groupServiceFactory.Create((useAppAuth) => this.msGraphAuthenticationCallback(identity));
                IMdmService mdmService = this.mdmServiceFactory.Create((useAppAuth) => this.msGraphAuthenticationCallback(identity));

                EnrollmentConfiguration configuration = await this.enrollmentService.GetLastKnownGoodConfigurationAsync();
                if (configuration == null)
                {
                    throw new ConfigurationNotFoundException("Last known good configuration could not be found for the tenant");
                }

                IEnumerable<OperationData> operations = configuration.PrototypeOperations;

                IOperationFactory operationFactory = new OperationFactory(
                            tenant,
                            configuration,
                            shard,
                            mdmService,
                            groupService,
                            updatesRepository,
                            embeddedResourceReader,
                            flightingResolver);

                IEnumerable<OperationData> exportableOps = from op in operations where (IsOperationExportableAsPS(op.Name)) select op;
                IEnumerable<IOperation> exportableOpsOrderedByPrecedence = exportableOps.Select(y => operationFactory.CreateOperation(y)).OrderBy(z => z.Precedence);

                if (Enumerable.Count(exportableOpsOrderedByPrecedence) == 0)
                {
                    return null;
                }
                else
                {
                    string BLANK = Environment.NewLine;
                    const string COPYRIGHT = @"
<#
.COPYRIGHT
Copyright (c) Microsoft Corporation. All rights reserved. Licensed under the MIT license.
See LICENSE in the project root for license information.
#>
";
                    const string SEP = "####################################################";
                    StringBuilder scriptBuilder = new StringBuilder(BLANK).AppendLine(COPYRIGHT).AppendLine(SEP).AppendLine(BLANK);

                    string startTranscriptScript = LoadStartTranscriptScript();
                    scriptBuilder.AppendLine(startTranscriptScript);
                    scriptBuilder.AppendLine(BLANK).AppendLine(SEP).AppendLine(BLANK);

                    IEnumerable<System.Linq.IGrouping<String, IOperation>> opsGroupedByType = exportableOpsOrderedByPrecedence.GroupBy((op) => op.Name);
                    foreach (var group in opsGroupedByType)
                    {
                        IOperation prototypeOp = group.ElementAt(0);
                        string script = prototypeOp.ExportPowershellScript();
                        scriptBuilder.AppendLine(script);
                        scriptBuilder.AppendLine(BLANK).AppendLine(SEP).AppendLine(BLANK);
                    }

                    string authScript = LoadTenantAuthPSScript();
                    scriptBuilder.AppendLine(authScript);
                    scriptBuilder.AppendLine(BLANK).AppendLine(SEP).AppendLine(BLANK);
                    foreach (var op in exportableOpsOrderedByPrecedence)
                    {
                        string script = op.ExportPowershellStub();
                        scriptBuilder.AppendLine(script);
                        scriptBuilder.AppendLine(BLANK).AppendLine(SEP).AppendLine(BLANK);
                    }

                    string stopTranscriptScript = LoadStopTranscriptScript();
                    scriptBuilder.AppendLine(stopTranscriptScript);

                    string finalScript = scriptBuilder.ToString();
                    Console.WriteLine(finalScript);
                    return finalScript;
                }
            }
        }



        private string LoadTenantAuthPSScript()
        {
            // string scriptFilePath = Path.Combine(System.AppDomain.CurrentDomain.BaseDirectory, $@"..\mWaaS.Services.Core\Domain\Services\PowershellScripts\Get_Auth.ps1");
            string scriptFilePath = "Get_Auth.ps1";
            string scriptText = embeddedResourceReader.ReadEmbeddedResourceContents(AssemblyType.Executing, scriptFilePath);
            return scriptText;
        }

        private string LoadStopTranscriptScript()
        {
            // string scriptFilePath = Path.Combine(System.AppDomain.CurrentDomain.BaseDirectory, $@"..\mWaaS.Services.Core\Domain\Services\PowershellScripts\StopTranscript.ps1");
            string scriptFilePath = "StopTranscript.ps1";
            string scriptText = embeddedResourceReader.ReadEmbeddedResourceContents(AssemblyType.Executing, scriptFilePath);
            return scriptText;
        }

        private string LoadStartTranscriptScript()
        {
            // string scriptFilePath = Path.Combine(System.AppDomain.CurrentDomain.BaseDirectory, $@"..\mWaaS.Services.Core\Domain\Services\PowershellScripts\StartTranscript.ps1");
            string scriptFilePath = "StartTranscript.ps1";
            string scriptText = embeddedResourceReader.ReadEmbeddedResourceContents(AssemblyType.Executing, scriptFilePath);
            return scriptText;
        }


        private Boolean IsOperationExportableAsPS(string opName)
        {
            var found = false;
            var names = Enum.GetNames(typeof(OperationExportedAsPowershell));
            foreach (var enumName in names)
            {
                if (enumName.Equals(opName, StringComparison.OrdinalIgnoreCase))
                {
                    found = true;
                    break;
                }
                else
                {
                    found = false;
                }
            }
            return found;
        }

        private async Task<CustomerConfigurableSetting> ExecuteCreateCustomerConfigurableSetting(
            OperationData od,
            CustomerTenant tenant,
            ITenantShard shard,
            ClaimsPrincipal identity,
            Func<bool, Task<string>> graphTokenDelegate,
            int version,
            string displayName,
            CustomerConfigurableSettingType type,
            Dictionary<string, string> properties)
        {
            //Get all policies of type, check if any are marked "In progress" if yes, cancel old one and create new one.
            IEnumerable<CustomerConfigurableSettingDeploymentTracker> settings = await shard.CustomerTenantRepository.GetCustomerConfigurableSettingsByType(type);
            if (od != null)
            {
                if (settings.Where(x => x.Status == CustomerConfigurableSettingStatus.InProgress).Count() != 0)
                {
                    await shard.CustomerTenantRepository.CancelCustomerConfigurableSetting(settings.Where(x => x.Status == CustomerConfigurableSettingStatus.InProgress).First().Id);
                }

                od.Id = await shard.CustomerTenantRepository.InsertTenantOperationAsync(od);

                await this.enrollmentService.MigrateSingleOperationAsync(tenant, identity, od, graphTokenDelegate);
                return await shard.CustomerTenantRepository.InsertCustomerConfigurableSettingAsync(identity, od.CorrelationId, type, version, displayName, JsonConvert.SerializeObject(properties));
            }
            throw new ArgumentNullException("Operation cannot be null");
        }

        private async Task<string> AuthenticateToMsGraphAsync(
            ClaimsPrincipal identity)
        {
            Debug.Assert(identity != null);

            return await this.msGraphAuthenticationCallback(identity);
        }
    }
}

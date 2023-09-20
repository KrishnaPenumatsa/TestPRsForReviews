using Microsoft.Graph;
using MMD.ARTService.Common.Constant;
using MMD.ARTService.Contracts;
using MMD.ARTService.Contracts.Enums;
using MMD.ARTService.Contracts.Evaluations;
using MMD.ARTService.Contracts.Evaluations.Enums;
using MMD.ARTService.Services.CloudCheck.Processors;
using MMD.ARTService.Services.Dependencies.Graph.Processor;
using MMD.Core;
using MMD.Core.Flighting;
using MMD.Core.Telemetry;

namespace MMD.ARTService.Services.CloudCheck.Assesements
{
    /// <summary>
    /// Mandatory check. 
    /// Look up tenant licenses(Azure AD> Licenses> All Products).  If tenant doesn’t have qualifying service plan, show message.
    /// The check looks up that at least one license of each of the below services plans exist on a tenant.
    /// </summary>
    public class LicensesAssessment : AssessmentBase
    {
        /// <summary>
        /// Qualifying service plan IDs. Retrieve the service plan IDs from:
        ///  https://docs.microsoft.com/en-us/azure/active-directory/users-groups-roles/licensing-service-plan-reference
        /// </summary>
        public static class ServicePlanId
        {
            public static readonly Guid WIN10_PRO_ENT_SUB = Guid.Parse("21b439ba-a0ca-424f-a6cc-52f954a5b111"); // Windows 10 Enterprise E3 (WIN10_PRO_ENT_SUB)
            public static readonly Guid OFFICESUBSCRIPTION = Guid.Parse("43de0ff5-c92c-492b-9116-175376d08c38"); // M365 Apps for Enterprise (OFFICESUBSCRIPTION)
            public static readonly Guid INTUNE_A = Guid.Parse("c1ec4a95-1f05-45b3-a911-aa3fa01094f5"); // Intune (INTUNE_A)
            public static readonly Guid WINDEFATP = Guid.Parse("871d91ec-ec1a-452b-a83f-bd76c7d770ef"); // Microsoft Defender for Endpoint (formerly MDATP: WINDEFATP)
            public static readonly Guid AAD_PREMIUM_P1 = Guid.Parse("41781fb2-bc02-4b7c-bd55-b576c07bb09d"); // Azure AD Premium 1 (AAD_PREMIUM_P1)
        }

        /// <summary>
        /// Qualifying Sku IDs. Retrieve the service plan IDs from:
        ///  https://docs.microsoft.com/en-us/azure/active-directory/users-groups-roles/licensing-service-plan-reference
        /// </summary>
        public static class SkuId
        {
            public static readonly Guid OFFICE_365_E3_SKU_ID = Guid.Parse("05e9a617-0261-4cee-bb44-138d3ef5d965");
            public static readonly Guid OFFICE_365_E5_SKU_ID = Guid.Parse("06ebc4ee-1bb5-47dd-8120-11324bc54e06");
            public static readonly Guid WINDOWS_E3_VDA_ONLY_SKU_ID = Guid.Parse("d13ef257-988a-46f3-8fce-f47484dd4550");
            public static readonly Guid WINDOWS_E3_SKU_ID = Guid.Parse("6a0f6da5-0b87-4190-a6ae-9bb5a2b9546a"); // Microsoft Defender for Endpoint (formerly MDATP: WINDEFATP)
            public static readonly Guid WINDOWS_E5_SKU_ID = Guid.Parse("488ba24a-39a9-4473-8ee5-19291e71b002"); // Azure AD Premium 1 (AAD_PREMIUM_P1)
            public static readonly Guid Windows_Enterprise_E3_Demo_Trial = Guid.Parse("48536fd3-7ce3-456b-b815-c0f6a19fd107");
            public static readonly Guid Microsoft_365_E3_Unattended_License_SKU_ID = Guid.Parse("c2ac2ee4-9bb1-47e4-8541-d689c7e83371");
            public static readonly Guid Microsoft_365_E3_HUB_SKU_ID = Guid.Parse("0c21030a-7e60-4ec7-9a0f-0042e0e0211a");
            public static readonly Guid Microsoft_365_E5_HUB_SKU_ID = Guid.Parse("db684ac5-c0e7-4f92-8284-ef9ebde75d33");
            public static readonly Guid Microsoft_365_E5_Calling_Minutes_SKU_ID = Guid.Parse("a91fc4e0-65e5-4266-aa76-4037509c1626");
            public static readonly Guid Microsoft_365_E5_Without_Audio_Conferencing_SKU_ID = Guid.Parse("cd2925a3-5076-4233-8931-638a8c94f773");
            public static readonly Guid Microsoft_365_E5_Without_Audio_Conferencing_HUB_SKU_ID = Guid.Parse("2113661c-6509-4034-98bb-9c47bd28d63c");
            public static readonly Guid TEST_Microsoft_365_E3_SKU_ID = Guid.Parse("23a55cbc-971c-4ba2-8bae-04cd13d2f4ad");
            public static readonly Guid TEST_Microsoft_365_E5_Without_Audio_Conferencing_SKU_ID = Guid.Parse("1362a0d9-b3c2-4112-bf1a-7a838d181c0f");
            public static readonly Guid Windows_10_11_Enterprise_E3_Sub_SKU_ID = Guid.Parse("cb10e6cd-9da4-4992-867b-67546b1db821");
            public static readonly Guid Windows_10_11_Enterprise_E5_Original_SKU_ID = Guid.Parse("1e7e1070-8ccb-4aca-b470-d7cb538cb07e");
            public static readonly Guid Microsoft_365_E5_Suite_features  = Guid.Parse("99cc8282-2f74-4954-83b7-c6a9a1999067");
            public static readonly Guid Microsoft_365_E3_Extra_Features = Guid.Parse("f5b15d67-b99e-406b-90f1-308452f94de6");
        }

        public const string ProvisioningSuccessStatus = "Success";

        private const AssessmentType LicensesAssessmentType = AssessmentType.Licenses;

        private static readonly IEnumerable<Guid> LicenseGuidListForP1Evaluation = new[] {
            ServicePlanId.INTUNE_A,
            ServicePlanId.AAD_PREMIUM_P1};

        private static readonly IEnumerable<Guid> LicenseGuidListForP2Evaluation = new[] {
            ServicePlanId.WIN10_PRO_ENT_SUB,
            ServicePlanId.OFFICESUBSCRIPTION,
            ServicePlanId.INTUNE_A,
            ServicePlanId.WINDEFATP,
            ServicePlanId.AAD_PREMIUM_P1};

        private static readonly IEnumerable<Guid> SkuIdListForP1EvaluationBeforeUpdate = new[] {
            SkuId.OFFICE_365_E3_SKU_ID,
            SkuId.OFFICE_365_E5_SKU_ID,
            SkuId.WINDOWS_E3_VDA_ONLY_SKU_ID,
            SkuId.WINDOWS_E3_SKU_ID,
            SkuId.WINDOWS_E5_SKU_ID,
            SkuId.Windows_Enterprise_E3_Demo_Trial,
            SkuId.Microsoft_365_E3_Unattended_License_SKU_ID,
            SkuId.Microsoft_365_E3_HUB_SKU_ID,
            SkuId.Microsoft_365_E5_HUB_SKU_ID,
            SkuId.Microsoft_365_E5_Calling_Minutes_SKU_ID,
            SkuId.Microsoft_365_E5_Without_Audio_Conferencing_SKU_ID,
            SkuId.Microsoft_365_E5_Without_Audio_Conferencing_HUB_SKU_ID,
            SkuId.TEST_Microsoft_365_E3_SKU_ID,
            SkuId.TEST_Microsoft_365_E5_Without_Audio_Conferencing_SKU_ID,
            SkuId.Windows_10_11_Enterprise_E3_Sub_SKU_ID,
            SkuId.Windows_10_11_Enterprise_E5_Original_SKU_ID
        };

        private static readonly IEnumerable<Guid> SkuIdListForP1Evaluation = new [] {
            SkuId.OFFICE_365_E3_SKU_ID,
            SkuId.OFFICE_365_E5_SKU_ID,
            SkuId.WINDOWS_E3_VDA_ONLY_SKU_ID,
            SkuId.WINDOWS_E3_SKU_ID,
            SkuId.WINDOWS_E5_SKU_ID,
            SkuId.Windows_Enterprise_E3_Demo_Trial,
            SkuId.Microsoft_365_E3_Unattended_License_SKU_ID,
            SkuId.Microsoft_365_E3_HUB_SKU_ID,
            SkuId.Microsoft_365_E5_HUB_SKU_ID,
            SkuId.Microsoft_365_E5_Calling_Minutes_SKU_ID,
            SkuId.Microsoft_365_E5_Without_Audio_Conferencing_SKU_ID,
            SkuId.Microsoft_365_E5_Without_Audio_Conferencing_HUB_SKU_ID,
            SkuId.TEST_Microsoft_365_E3_SKU_ID,
            SkuId.TEST_Microsoft_365_E5_Without_Audio_Conferencing_SKU_ID,
            SkuId.Windows_10_11_Enterprise_E3_Sub_SKU_ID,
            SkuId.Windows_10_11_Enterprise_E5_Original_SKU_ID,
            SkuId.Microsoft_365_E5_Suite_features,
            SkuId.Microsoft_365_E3_Extra_Features
        };

        private static readonly IEnumerable<Guid> SkuIdListForP2Evaluation = new HashSet<Guid>();

        private readonly Type LicenseGraphType = typeof(LicenseGraphPreprocessor);

        private readonly IPreProcessor licenseGraphPreProcessor;
        private readonly IFlightingResolver flightingResolver;

        public LicensesAssessment(
            IEnumerable<IPreProcessor> preProcessorsList,
            ITelemetryService telemetryService,
            IFlightingResolver flightingResolver) : base(telemetryService)
        {
            Guard.NotNullOrEmpty(preProcessorsList, nameof(preProcessorsList));
            Guard.NotNull(flightingResolver, nameof(flightingResolver));

            this.flightingResolver = flightingResolver;
            licenseGraphPreProcessor = preProcessorsList.Single(p => LicenseGraphType.Equals(p.GetType()));
        }

        public override IEnumerable<EvaluationType> GetEvaluationTypeList() => new List<EvaluationType>
        {
            EvaluationType.BusinessCheck,
            EvaluationType.P2,
            EvaluationType.P1,
        };

        public override IPreProcessor GetPreProcessor() => licenseGraphPreProcessor;

        public override Type GetPreProcessorType() => LicenseGraphType;

        public override AssessmentType GetAssessmentType() => LicensesAssessmentType;

        internal override Task<AssessmentResult> CheckAssessmentResultAsync(
            EvaluationContext evaluationContext,
            object preProcessorResponse)
        {
            Guard.NotNull(preProcessorResponse, nameof(preProcessorResponse));

            using var telemetryOperation = telemetryService.CreateTelemetryOperation(
               nameof(LicensesAssessment),
               new Dictionary<string, object>()
               {
                    { MetricsConstants.EvaluationTypeMetricsKey , evaluationContext.EvaluationType }
               });

            var subscribedSkus = (IEnumerable<SubscribedSku>)preProcessorResponse;

            if (!subscribedSkus.Any())
            {
                telemetryOperation.TrackTrace($"subscribedSkus is Empty");
                return Task.FromResult(new AssessmentResult
                {
                    AssessmentType = LicensesAssessmentType,
                    Result = Result.NotReady,
                    EndTimestamp = DateTime.UtcNow
                });
            }

            var servicePlanInfos = new List<ServicePlanInfo>() { };
            var skuIds = new List<Guid>() { };

            foreach (var sku in subscribedSkus)
            {
                if (sku.SkuId.HasValue)
                {
                    skuIds.Add(sku.SkuId.Value);
                }
                servicePlanInfos.AddRange(sku.ServicePlans);
            }

            if (!servicePlanInfos.Any())
            {
                telemetryOperation.TrackTrace($"servicePlanInfos is Empty");
                return Task.FromResult(new AssessmentResult
                {
                    AssessmentType = LicensesAssessmentType,
                    Result = Result.NotReady,
                    EndTimestamp = DateTime.UtcNow
                });
            }

            var ret = CheckLicenseAssessment(
                skuIds,
                servicePlanInfos,
                evaluationContext.EvaluationType);

            return ret;
        }

        /// <summary>
        /// Change the value from planName to planId
        /// </summary>
        /// <param name="servicePlans"></param>
        /// <param name="evaluationType"></param>
        /// <returns></returns>
        internal async Task<AssessmentResult> CheckLicenseAssessment(
            IEnumerable<Guid> skuIds,
            IEnumerable<ServicePlanInfo> servicePlans,
            EvaluationType evaluationType)
        {
            Guard.NotNullOrEmpty(servicePlans, nameof(servicePlans));

            using var telemetryOperation = telemetryService.CreateTelemetryOperation(nameof(CheckLicenseAssessment));

            var updateLicense = await flightingResolver.IsFlightEnabled(FlightModeConstants.LicenseUpdateForART0726);
            var validSkuIds = (evaluationType == EvaluationType.P1)
                ? (updateLicense
                        ? SkuIdListForP1Evaluation
                        : SkuIdListForP1EvaluationBeforeUpdate)
                : SkuIdListForP2Evaluation;

            var expectedLicenseGuidList = (evaluationType == EvaluationType.P1)
                ? LicenseGuidListForP1Evaluation
                : LicenseGuidListForP2Evaluation;
            var actualLicenseGuidList = new List<Guid>();

            foreach (var servicePlan in servicePlans)
            {
                var planId = servicePlan.ServicePlanId;
                if (ProvisioningSuccessStatus.Equals(servicePlan.ProvisioningStatus)
                    && planId is not null)
                {
                    actualLicenseGuidList.Add((Guid)planId);
                }
            }

            var provisionedLicenses = expectedLicenseGuidList.Where(license => actualLicenseGuidList.Contains(license))
                .ToList();

            var provisionedLicensesNames = servicePlans.Where(
                    servicePlan =>
                        servicePlan.ServicePlanId is not null &&
                        provisionedLicenses.Contains(servicePlan.ServicePlanId.Value))
                .Select(x => x.ServicePlanName)
                .ToList();

            var missingLicenses = expectedLicenseGuidList.Where(license => !actualLicenseGuidList.Contains(license))
                .ToList();

            bool hasValidSkuId = !validSkuIds.Any()
                || (skuIds is not null && skuIds.Any(skuId => validSkuIds.Contains(skuId)));

            string provisionedLicensesString = string.Join(ServiceConstants.COMMA_SEPARATOR, provisionedLicensesNames);

            telemetryOperation.AddPropertiesFromDictionary(new Dictionary<string, object?>()
            {
                {"MissingLicense",  string.Join(ServiceConstants.COMMA_SEPARATOR, missingLicenses) },
                {"ProvisionedLicense", provisionedLicensesString },
                {"ValidSkuIds", string.Join(ServiceConstants.COMMA_SEPARATOR, validSkuIds) },
                {"ProvisionedSkuIds", string.Join(ServiceConstants.COMMA_SEPARATOR, skuIds ?? Array.Empty<Guid>()) },
            });

            return (new AssessmentResult
            {
                AssessmentType = LicensesAssessmentType,
                Result = missingLicenses.Any() || !hasValidSkuId ? Result.NotReady : Result.Ready,
                Value = provisionedLicensesString,
                EndTimestamp = DateTime.UtcNow
            });
        }
    }
}

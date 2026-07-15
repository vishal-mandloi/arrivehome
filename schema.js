const { AUTOMATION_TYPE_OPTIONS, US_STATE_CODES, LOAN_STATUS_OPTIONS, EEP_PROCESSING_STATUS_OPTIONS, RECONCILIATION_STATUS_OPTIONS, LOAN_DOCUMENT_TYPES, SECOND_MORTGAGE_OWNERSHIP_STATUSES } = require('./schema_options.js');

// This will only accept reasonable email addresses. It is not RFC 5322 compliant, but that should never matter for our use
// https://colinhacks.com/essays/reasonable-email-regex https://www.npmjs.com/package/zod
const EMAIL_REGEXP = /^([A-Z0-9_+-]+\.?)*[A-Z0-9_+-]@([A-Z0-9][A-Z0-9-]*\.)+[A-Z]{2,}$/i;

// This will accept numbers like 8881231234, +18881231234, 888-123-1234, or (888) 123 1234
const PHONE_NUMBER_REGEXP = /^((\+1)?\d{10})|(\d{3}-\d{3}-\d{4})|(\(\d{3}\) \d{3} \d{4})$/;

module.exports = {
    User: {
        extend: true,
        fields: {
            __postgresId: { type: 'integer' },
            __userType: { type: 'string', options: ['Internal', 'Correspondent'] },
            __correspondent: { type: 'Correspondent' },
            __internalRoles: { type: 'string[]', options: ['Employee', 'Manager', 'Sales Manager', 'CSPortal', 'PolicyHub'] },
            __correspondentRoles: { type: 'string[]', options: ['View Loans', 'Register Loans', 'Lock Loans', 'Manage Users', 'View Purchase Advice'] },
            __policyhubGuid: { type: 'string', label: 'PolicyHub GUID' },
            __policyhubGroups: { type: 'PolicyhubGroup[]', label: 'PolicyHub Groups' },
            __servicingMemberPhoneNumber: { type: 'phone' },
            __canSignTrfaAllonge: { type: 'boolean' },
            __canSignHomegenAllonge: { type: 'boolean' },
            __trfaAllongeSigningCapacity: { type: 'string' },
            __homegenAllongeSigningCapacity: { type: 'string' },
        },
        indexes: [
            { fields: ['__postgresId'] },
        ]
    },

    Correspondent: {
        fields: {
            _id: { type: 'id' },
            postgresId: { type: 'integer' },
            name: { type: 'string' },
            nmlsNumber: { type: 'string', label: 'NMLS Number' },
            address: { type: 'string' },
            city: { type: 'string' },
            state: { type: 'string' , options: US_STATE_CODES },
            zip: { type: 'string' },
            createdAt: { type: 'datetime' },
            primaryContact: { type: 'User' },
            parentCorrespondent: { type: 'Correspondent' },
            accountExecutive: { type: 'User' },
            prePurchaseConditionsContacts: { type: 'User[]' },
            defaultOriginatingOrgId: { type: 'string' },
            loanDepotWorkflowTypeEnabled: { type: 'boolean' },
            blueWaterWhiteLabelDeal: { type: 'BlueWaterDeal' },
            blueWaterWhiteLabelOutputSftpPath: { type: 'string' },
            blueWaterWhiteLabelInputSftpPath: { type: 'string' },
            blueWaterDpaDeal: { type: 'BlueWaterDeal' },
            blueWaterDpaOutputSftpPath: { type: 'string' },
            blueWaterDpaInputSftpPath: { type: 'string' },
            enabledProductTypes: { type: 'string[]', options: ['DPA', 'EEP', 'White Label'] },
            enableDpaOnePercentAbove: { type: 'boolean' },
            enableTwoPointFivePercentDpa: { type: 'boolean', label: 'Enable 2.5% DPA' },
            enableWhiteLabelVaProgram: { type: 'boolean' },
            enableBulkUploadTrailingDocs: { type: 'boolean' },
            useCustomRateSheet: { type: 'boolean' },
            minInterestRate: { type: 'decimal', format: 'percent' },
            maxInterestRate: { type: 'decimal', format: 'percent' },
            mountainWestContractDate: { type: 'dateonly' },
            mountainWestGoLiveDate: { type: 'dateonly' },
            eepPurchaseFirstMortgageWithoutDpa: { type: 'boolean' },
            dpaPurchaseFirstMortgageWithoutDpa: { type: 'boolean' },
            secondMortgageRepurchaseRecipientEmail: { type: 'string' },
            solarProvider: { type: 'string', options: ['Arcasa', 'Solify'] },

            whiteLabelProcessingFee: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            whiteLabelAdministrativeFeeBasisPoints: { type: 'decimal' },
            whiteLabelAdministrativeFeeCustomPoolPickupPercentage: { type: 'decimal', format: 'percent' },
            whiteLabelServiceFeeThreePointFiveRepayableBasisPoints: { type: 'decimal' },
            whiteLabelServiceFeeFivePointZeroRepayableBasisPoints: { type: 'decimal' },
            whiteLabelDiscountPurchasePricePercentage: { type: 'decimal', format: 'percent' },

            secondMortgageRepurchaseAdditionalFee: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },

            enable3Unit: { type: 'boolean' },
            enable4Unit: { type: 'boolean' },

            enable60DayLock: { type: 'boolean' },

            earlyPaymentDefaultTerm: { type: 'string[]', options: ['6 Months'] },

            eepUnderwriterOverride: { type: 'User' },

        },
        indexes: [
            { fields: ['postgresId'] },
            { fields: ['blueWaterWhiteLabelDealId'], partial: { blueWaterWhiteLabelDealId: { $type: 'objectId' } }, unique: true },
            { fields: ['blueWaterDpaDealId'], partial: { blueWaterDpaDealId: { $type: 'objectId' } }, unique: true },
        ]
    },

    Loan: {
        fields: {
            _id: { type: 'id' },
            _revision: { type: 'integer' },
            ahLoanNumber: { type: 'string', label: 'AH Loan Number' },
            workflowType: { type: 'string', options: ['CSPortal', 'Loan Depot', 'White Label', 'DPA', 'EEP', 'Solar Program'] },
            postgresId: { type: 'integer' },
            postgresUpdatedAt: { type: 'datetime' },
            blueWaterId: { type: 'integer' },
            blueWaterUpdatedDate: { type: 'dateonly' }, // deprecated, should remove after migration runs
            blueWaterUpdatedAt: { type: 'datetime' },
            blueWaterErrors: { type: 'string[]' },
            postgresErrors: { type: 'string[]' },
            createdBy: { type: 'User' },
            createdAt: { type: 'datetime' },
            correspondent: { type: 'Correspondent' },
            lenderLoanNumber: { type: 'string' }, // should this be correspondent loan number?

            // Start Fields Mapped from Indexed Close Loan Package
            escrowMonthlyPayment: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            insuranceMonthlyPayment: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            homeownersInsuranceMonthlyPayment: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            floodInsuranceMonthlyPayment: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            floodInsuranceAnnualPayment: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            stormInsuranceMonthlyPayment: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            miscInsuranceMonthlyPayment: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            countyMonthlyPayment: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            cityMonthlyPayment: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            lienMonthlyPayment: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            pmiMonthlyPayment: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            totalMonthlyLoanPayment: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            interestCollectedAtClosing: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            pointsPaidByBorrower: { type: 'decimal', decimalPlaces: 3 },
            loanPurpose: { type: 'string' },
            fhaUpfrontMortgageInsuranceAmount: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            prepaidFloodInsurance: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            prepaidTax1: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            prepaidTax2: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },

            prepayPenaltyIndicator: { type: 'boolean' },
            prepayPenaltyDescription: { type: 'string' },
            mersRegistrationDate: { type: 'dateonly' },
            mersRegistrationFlag: { type: 'boolean' },
            lateChargeCode: { type: 'string', options: [
                { value: 'A', label: 'A - Percentage of Principal & Interest (with dollar amount limits)' },
                { value: 'B', label: 'B - Percentage of Net Payment (with percentage amount limit)' },
                { value: 'C', label: 'C - Percentage of Principal Balance (with dollar amount limits)' },
                { value: 'D', label: 'D - Fixed Dollar Amount (equal to % of Principal & Interest)' },
                { value: 'E', label: 'E - Fixed Dollar Amount (equal to % of Payment)' },
                { value: 'F', label: 'F - Fixed Dollar Amount (equal to % of Principal Balance)' },
                { value: 'G', label: 'G - Percentage of Delinquent Interest (from the most recent payment)' },
                { value: 'H', label: 'H - Percentage of Total Payment (with maximum and minimum limits)' },
                { value: 'K', label: 'K - Fixed Dollar Amount (consumer only)' },
                { value: 'L', label: 'L - Percentage of Minimum Payment Requirement (consumer only)' },
            ] },
            lateChargePercentage: { type: 'decimal', format: 'percent', decimalPlaces: 2 },
            interestOnlyFlag: { type: 'boolean' },
            interestOnlyExpirationDate: { type: 'dateonly' },
            balloonIndicator: { type: 'boolean' },
            balloonTerm: { type: 'string' },
            armIndicator: { type: 'boolean' },

            floodSuffix: { type: 'string' },
            floodMapDate: { type: 'dateonly' },
            floodContractType: { type: 'string' },

            nextInstallmentHazardInsurance: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            nextInstallmentFloodInsurance: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            nextInstallmentPropertyTax: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },

            fhaPayee: { type: 'string' },
            fhaSectionCode: { type: 'string' },
            fhaAdpCode: { type: 'string' },

            hazardInsuranceTypeCode: { type: 'string' },
            hazardInsuranceCarrier: { type: 'string' },
            hazardInsuranceCoverageAmount: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            hazardInsurancePremiumAmount: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            floodInsuranceTypeCode: { type: 'string' },
            floodInsuranceCarrier: { type: 'string' },
            floodInsuranceExpirationDate: { type: 'dateonly' },
            floodInsurancePolicyNumber: { type: 'string' },
            floodInsuranceEscrowStatus: { type: 'string' },
            windInsuranceTypeCode: { type: 'string' },
            windInsuranceCarrier: { type: 'string' },
            windInsuranceCoverageAmount: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            windInsuranceDeductibleAmount: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            windInsurancePremiumAmount: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            windInsuranceExpirationDate: { type: 'dateonly' },
            windInsurancePolicyNumber: { type: 'string' },
            windInsuranceEscrowStatus: { type: 'string' },
            miscHazardInsuranceTypeCode: { type: 'string' },
            miscHazardInsuranceCarrier: { type: 'string' },
            miscHazardInsuranceCoverageAmount: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            miscHazardInsurancePremiumAmount: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            miscHazardInsuranceExpirationDate: { type: 'dateonly' },
            miscHazardInsurancePolicyNumber: { type: 'string' },
            miscHazardInsuranceEscrowStatus: { type: 'string' },
            // End Fields Mapped from Indexed Close Loan Package

            purchaseClearingAssignedTo: { type: 'User' },
            purchaseClearingAssistantAssignedTo: { type: 'User' },
            purchaseClearingIntakeAuditReadyDate: { label: 'Purchase Clearing Intake Audit/Ready Date', type: 'dateonly' },
            purchaseClearingIntakeAuditCompletionDate: { label: 'Purchase Clearing Intake Audit/Completion Date', type: 'dateonly' },

            fundingPreCheckAssignedTo: { type: 'User' },
            fundingWireAssignedTo: { type: 'User' },
            mersTransferAssignedTo: { type: 'User' },

            fhaCaseNumber: { type: 'string', label: 'FHA Case Number' },
            productType: { type: 'string', options: ['DPA', 'EEP', 'White Label', 'Solar Program'] },
            dpaRepaymentType: { type: 'string', options: ['Repayable', 'Forgivable'], label: 'DPA Repayment Type' },
            purchasePrice: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },

            status: { type: 'string', options: LOAN_STATUS_OPTIONS },

            loanComments: { type: 'LoanComment[]' },
            loanCallLogs: { type: 'LoanCallLog[]' },
            exceptions: { type: 'Exception[]' },

            postgresStatus: { type: 'string' },

            registeredAt: { type: 'datetime' },
            pendingLockAt: { type: 'datetime' },
            lockedAt: { type: 'datetime' },
            postClosingTeamReviewAt: { type: 'datetime' },
            purchaseReviewAt: { type: 'datetime' },
            dpaFundsRequestedAt: { type: 'datetime' },
            pendingConditionsAt: { type: 'datetime' },
            allConditionsClearedAt: { type: 'datetime' },
            purchasedAt: { type: 'datetime' },
            securitizedAt: { type: 'datetime' },
            trailingDocsAt: { type: 'datetime' },
            postPurchaseConditionsAt: { type: 'datetime' },
            cancelledAt: { type: 'datetime' },
            initializedAt: { type: 'datetime' },
            deniedAt: { type: 'datetime' },
            onHoldAt: { type: 'datetime' },

            approvalAt: { type: 'datetime' },
            suspendedAt: { type: 'datetime' },
            finalApprovalAt: { type: 'datetime' },
            clearToCloseAt: { type: 'datetime' },
            closingAt: { type: 'datetime' },
            closedAt: { type: 'datetime' },
            postClosingAt: { type: 'datetime' },
            investorDeliveryAt: { type: 'datetime' },
            pendingUwReviewAt: { type: 'datetime' },
            closedLoanPackageReceivedAt: { type: 'datetime' },
            closedLoanPackageIndexedAt: { type: 'datetime' },
            closedLoanPackageBoardedAt: { type: 'datetime' },
            collateralReceivedAt: { type: 'datetime' },
            approvedForReimbursementAt: { type: 'datetime' }, // TODO: remove this
            approvedForPurchaseAt: { type: 'datetime' },
            reimbursedAt: { type: 'datetime' },
            reconciledAt: { type: 'datetime' }, // TODO: remove this

            // Solar Program status timestamps
            solarQuoteReceivedAt: { type: 'datetime' },
            solarQuoteAcceptedAt: { type: 'datetime' },
            solarPendingCloseAt: { type: 'datetime' },
            solarFundsSentAt: { type: 'datetime' },
            solarPaidInFullAt: { type: 'datetime' },

            readyForBoardingAt: { type: 'datetime' },

            purchaseClearingReviewCompletedAt: { type: 'datetime' },
            
            secondMortgageMersTobToTrfaAt: { type: 'datetime', label: '2nd Mortgage MERS TOB To TRFA At' },
            secondMortgageMersTosServicerToTrfaAt: { type: 'datetime', label: '2nd Mortgage MERS TOS Servicer To TRFA At' },
            secondMortgageMersTosSubservicerToBsiAt: { type: 'datetime', label: '2nd Mortgage MERS TOS Subservicer To BSI At' },

            reconciliationReportReceivedAt: { type: 'datetime' },
            invoiceGeneratedAt: { type: 'datetime' }, // We're not using this field anymore
            correspondentFeeWireReceivedAt: { type: 'datetime' },
            trfaDpaReimbursementWireSentAt: { type: 'datetime' },
            trailingDocsWillBeACopy: { type: 'boolean' },

            basePrice: { type: 'decimal' }, // TODO: I want a more specific name
            totalPrice: { type: 'decimal' }, // TODO: I want a more specific name
            loanPriceAdjustments: { type: 'LoanPriceAdjustment[]' },

            processorReviewedDate: { type: 'dateonly' },
            submittedToUwDate: { type: 'dateonly' },
            uwApprovedDate: { type: 'dateonly' },
            clearToCloseDate: { type: 'dateonly' },

            propertyAddress: { type: 'string' },
            propertyCity: { type: 'string' },
            propertyState: { type: 'string', options: US_STATE_CODES },
            propertyZip: { type: 'string' },
            propertyCounty: { type: 'string' }, // should we do FIPS right from the start?
            propertyType: { type: 'string', options: ['Attached', 'Condo', 'MF Home', 'SFR', 'PUD', '2-Unit', '3-Unit', '4-Unit', 'Multiwide'] },
            numberOfUnits: { type: 'integer' },
            
            floodCertificateNumber: { type: 'string' },
            floodCertificateStatus: { type: 'string' },
            floodProgram: { type: 'string' },
            floodDeterminationDate: { type: 'dateonly' },
            floodFirmDate: { type: 'dateonly' },
            floodPanelNumber: { type: 'string' },
            floodRequired: { type: 'boolean', tristate: true },
            floodZone: { type: 'string', options: ['Zone A', 'Zone AO', 'Zone AH', 'Zone A1-A30', 'Zone AE', 'Zone A99', 'Zone AR', 'Zone AR/AE', 'Zone AR/AO', 'Zone AR/A1-A30', 'Zone AR/A', 'Zone V', 'Zone VE', 'Zones V1-V30', 'Zone B', 'Zone X', 'Zone C'] },
            floodInsuranceCoverageAmount: { type: 'decimal' },
            floodInsurancePremiumAmount: { type: 'decimal' },
            floodInsurancePremiumDueDate: { type: 'dateonly' },
            floodInsuranceCompanyName: { type: 'string' },
            floodCommunityNumber: { type: 'string' },
            floodCommunityDate: { type: 'dateonly' },
            femaDisaster: { type: 'boolean', tristate: true },

            additionalInterestedParties: { type: 'string' },

            hazardInsuranceCompanyName: { type: 'string' },
            hazardInsurancePolicyNumber: { type: 'string' },
            hazardInsuranceExpirationDate: { type: 'dateonly' },
            hazardInsurancePremiumAmount: { type: 'decimal' },
            hazardInsurancePremiumDueDate: { type: 'dateonly' },
            hazardInsuranceCoverageAmount: { type: 'decimal' },

            borrowers: { type: 'Borrower[]' },

            liabilities: { type: 'Liability[]' },
            incomes: { type: 'Income[]' },
            incomeDetails: { type: 'IncomeDetail[]' },
            assets: { type: 'Asset[]' },

            downPayment: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 0 },
            governmentEntityFhaLoanAmount: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 0 },
            totalIncome: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            totalLiabilitiesBalance: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            totalLiabilitiesMonthlyPayment: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            totalAssetsBalance: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            monthlyHazardInsurance: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            monthlyRealEstateTaxes: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            monthlyMortgageInsurance: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            otherExpenses: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            housingTotal: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            totalObligations: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },

            firstMortgageType: { type: 'string', options: ['FHA', 'VA'] },
            firstMortgageMinNumber: { type: 'string' },
            firstMortgageNoteDate: { type: 'dateonly' },
            firstMortgageUnpaidBalance: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            firstMortgageInterestRate: { type: 'decimal', format: 'percent' },
            firstMortgageFirstPaymentDueDate: { type: 'dateonly' },
            firstMortgageMaturityDate: { type: 'dateonly' },
            firstMortgageBaseLoanAmount: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 0 },
            firstMortgageTotalLoanAmount: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 0 },
            firstMortgageLateChargeMinDollarAmount: { type: 'decimal' },
            firstMortgageLateChargeMaxDollarAmount: { type: 'decimal' },
            firstMortgageAnnualPercentageRate: { type: 'decimal' },
            firstMortgageUniversalLoanIndicator: { type: 'string' },
            firstMortgageLateFeeRate: { type: 'decimal' },
            firstMortgageLoanToValueRatio: { type: 'decimal', format: 'percent' },
            firstMortgageDelinquencyStatus: { type: 'string', options: ['0-29', '30-59', '60-89', '90+'] },
            firstMortgageOwnershipStatus: { type: 'string', options: [
                'Pending Purchase',
                'Purchased',
                'Pending Sale',
                'Sold',
                'Pending Securitization',
                'Securitized',
                'Paid In Full',
                'Repurchased',
                'USF',
                'USF Securitized',
            ] },
            firstMortgageRepurchasedAt: { type: 'datetime' },
            firstMortgageWarehouseBank: { type: 'WarehouseBank' },
            firstMortgagePurchasedWithoutDpa: { type: 'boolean' },
            firstMortgageNoteTrackingNumber: { type: 'string' },
            firstMortgageTerm: { type: 'integer' }, // this is number of months

            secondMortgageMinNumber: { type: 'string' },
            secondMortgageNoteDate: { type: 'dateonly' },
            secondMortgageOriginationDate: { type: 'dateonly' },
            secondMortgageUpb: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 }, // Change this to secondMortgageUnpaidBalance
            secondMortgageInterestRateMatchesFirst: { type: 'boolean' }, // This applies to VA loans
            secondMortgageInterestRate: { type: 'decimal', format: 'percent' },
            secondMortgageFirstPaymentDueDate: { type: 'dateonly' },
            secondMortgageNextPaymentDueDate: { type: 'dateonly' },
            secondMortgageLastPaymentMadeDate: { type: 'dateonly' },
            secondMortgageMaturityDate: { type: 'dateonly' },
            secondMortgagePaidInFullDate: { type: 'dateonly' },
            secondMortgageDaysLate: { type: 'integer', commas: true },
            secondMortgagePAndIPayment: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            secondMortgageMonthlyPayment: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            secondMortgageLateFeeRate: { type: 'decimal' },
            secondMortgageLateChargeMinDollarAmount: { type: 'decimal' },
            secondMortgageLateChargeMaxDollarAmount: { type: 'decimal' },
            secondMortgageLateGracePeriodDays: { type: 'integer' },
            secondMortgageTerm: { type: 'integer' }, // this is number of months
            secondMortgageNoteTrackingNumber: { type: 'string' },

            closingDate: { type: 'dateonly' },
            dpaAmount: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 0  },
            dpaPercent: { type: 'decimal', format: 'percent' },
            dpaOnePercentAbove: { type: 'boolean' },
            appraisedValue: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 0 },
            currentPropertyValue: { type: 'decimal', format: 'dollars', commas: true },
            originalAppraisalDate: { type: 'dateonly' },
            combinedLoanToValueRatio: { type: 'decimal', format: 'percent' },

            health: { type: 'string', options: ['Green', 'Yellow', 'Red'] },
            healthReason: { type: 'string', options: ['Awaiting VN Review', 'Missing Blue Water Docs', 'Awaiting Purchase Review'] },

            hasBsiServicingRecords: { type: 'boolean' },

            originatingOrgId: { type: 'string' },
            investorOrgId: { type: 'string' },
            secondMortgageServicerOrgId: { type: 'string' },
            secondMortgageSubservicerOrgId: { type: 'string' },
            secondMortgageSubservicerName: { type: 'string' },

            blueWaterAllDocumentsReceived: { type: 'boolean' },
            blueWaterMissingDocuments: { type: 'string[]' },

            loanOfficerName: { type: 'string' },
            loanOfficerEmail: { type: 'string', validate: EMAIL_REGEXP },
            loanOfficerPhoneNumber: { type: 'string', validate: PHONE_NUMBER_REGEXP },
            loanOfficerNmlsNumber: { type: 'string' },
            manualUnderwrite: { type: 'boolean', tristate: true },
            lockRequestedAt: { type: 'datetime' },
            lockPeriod: { type: 'string', options: ['15', '30', '45', '60'] },
            lockExpirationDate: { type: 'dateonly' },
            incomeGreaterThan160PercentOfAmi: { type: 'boolean', tristate: true, label: 'Income > 160% of AMI' },

            governmentEntity: { type: 'string', options: ['Tule River Finance Agency'] },
            feeSimpleOwnership: { type: 'boolean', tristate: true },
            borrower4506tIndicator: { type: 'boolean', tristate: true },
            fhaCaseAssignmentDate: { type: 'dateonly' },
            
            monthlyTotalIncome: { type: 'decimal' },
            totalBackEndDebtToIncomeRatio: { type: 'decimal', format: 'percent' },
            totalFrontEndDebtToIncomeRatio: { type: 'decimal', format: 'percent' },
            mortgagePaymentToIncomeRatio: { type: 'decimal', format: 'percent' },

            totalPitiPayment: { type: 'decimal' },
            purchaseAdviceSentDate: { type: 'dateonly' },
            vorInFile: { type: 'boolean', tristate: true },
            reservesInMonths: { type: 'decimal' },
            ausDecision: { type: 'string' }, // TODO: define options
            finalQualifyingCreditScore: { type: 'decimal' },
            paymentShock: {  type: 'decimal' },
            totalAssets: { type: 'decimal' },
            fundsToCloseDocumented: { type: 'boolean', tristate: true },
            programParticipationPercentage: { type: 'decimal' },
            verifiedAssets: { type: 'decimal' },
            sellerConcessions: { type: 'decimal' },
            estimatedCashToCloseLe: { type: 'decimal' },
            warehouseDrawRequestDate: { type: 'dateonly' },
            excludedFromDrawRequest: { type: 'boolean' },
            isNoteLeveraged: { type: 'boolean' },
            creditor: { type: 'string', options: ['Bank of the Sierra'] },
            usfLoanNumber: { type: 'string' },
            noteAtCreditorDate: { type: 'dateonly' },
            specPayup: { type: 'decimal' },
            executionPrice: { type: 'decimal' },
            gnmSecurityPrice: { type: 'decimal' },
            tbaPrice: { type: 'decimal' },
            whiteLabelInvoicedDate: { type: 'dateonly' },
            momIndicator: { type: 'boolean', tristate: true },
            micReceivedDate: { type: 'dateonly' },
            warehouseBank: { type: 'string', options: ['Bank of the Sierra', 'Flagstar', 'Southstar'] }, // TODO: this should probably be secondMortgageWarehouseBank
            warehousePrincipalBalance: { type: 'decimal', format: 'dollars', commas: true },
            epd: { type: 'boolean', tristate: true },
            loanPerformance: { type: 'string', options: ['30+ Delinq', '60+ Deling', '90+ Delinq', '120+ Delinq'] },
            netReconciliationPrice: { type: 'decimal' },
            netSecuritizationPrice: { type: 'decimal' },
            essexLoanNumber: { type: 'string' },
            bsiLoanNumber: { type: 'string' },
            dpaReimbursementDate: { type: 'dateonly' },
            secondMortgageSoldDate: { type: 'dateonly' },
            secondMortgageInvoiceTradeNumber: { type: 'string' },
            secondMortgageSoldTo: { type: 'string' },
            secondLienReleaseDate: { type: 'dateonly' },
            investorBlockNumber: { type: 'string' },
            amountFinancedTil: { type: 'decimal' },
            disbursementDate: { type: 'dateonly' },
            discountPoints: { type: 'decimal' },
            escrowWaiverIndicator: { type: 'boolean', tristate: true },
            escrowFeeTotalAmount: { type: 'decimal' },
            escrowTaxFlag: { type: 'boolean', tristate: true },
            finalTilDisclosureDate: { type: 'dateonly' },
            financeCharge: { type: 'decimal' },
            lenderCredits: { type: 'decimal' },
            originationCharges: { type: 'decimal' },
            principalReduction: { type: 'decimal' },
            realEstateBrokerComissionsBuyer: { type: 'decimal' },
            realEstateBrokerComissionsSeller: { type: 'decimal' },
            salesContractAmount: { type: 'decimal' },
            sellerName: { type: 'string' },
            settlementAgentContact: { type: 'string' },
            settlementAgentEmail: { type: 'string', validate: EMAIL_REGEXP },
            settlementAgentName: { type: 'string' },
            settlementAgentPhone: { type: 'string', validate: PHONE_NUMBER_REGEXP },
            settlementCompanyAddress: { type: 'string' },
            totalOriginationAndDiscountPoints: { type: 'decimal' },
            upfrontMip: { type: 'decimal' },
            cityTaxAmount: { type: 'decimal' },
            cityTaxBillCode: { type: 'string' },
            cityTaxDisbursementAmount: { type: 'decimal' },
            cityTaxDisbursementDate: { type: 'dateonly' },
            cityTaxPayee: { type: 'string' },
            countyTaxAmount: { type: 'decimal' },
            countyTaxBillCode: { type: 'string' },
            countyTaxDisbursementAmount: { type: 'decimal' },
            countyTaxDisbursementDate: { type: 'dateonly' },
            countyTaxId: { type: 'string' },
            countyTaxIsd: { type: 'string' },
            countyTaxPayee: { type: 'string' },
            schoolTaxBillCode: { type: 'string' },
            schoolTaxDisbursementAmount: { type: 'decimal' },
            schoolTaxDisbursementDate: { type: 'dateonly' },
            schoolTaxId: { type: 'string' },
            schoolTaxIsd: { type: 'string' },
            schoolTaxPayee: { type: 'string' },            

            initialEscrowBalance: { type: 'decimal' },
            miPaymentAmount: { type: 'decimal' },
            monthlyEscrowPropertyInsurance: { type: 'decimal' },
            monthlyEscrowTax: { type: 'decimal' },

            wiringAccountNumber: { type: 'string' },
            wiringRoutingNumber: { type: 'string' },

            mortgageCreditCertificateIndicator: { type: 'boolean', tristate: true },
            newLienAmount: { type: 'decimal' }, // unused
            parcelNumber: { type: 'string' },
            censusTract: { type: 'string' },

            solarProvider: { type: 'string', options: ['Arcasa', 'Solify'] },
            solarQuoteRequestedAt: { type: 'datetime' },
            solarQuoteRequestError: { type: 'string' },
            totalSolarCostLineItem: { type: 'decimal', format: 'dollars', commas: true },
            totalSolarRebateAvailable: { type: 'decimal', format: 'dollars', commas: true },
            amountOfSolarRebateAccepted: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 0 },
            amountOfSolarRebateAppliedToClosingCosts: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 0 },
            solarQuoteAcceptedAmountsSentToProviderAt: { type: 'datetime' },
            solarQuoteAcceptedAmountsSentToProviderError: { type: 'string' },
            solifyLeadId: { type: 'string' },
            solarEstimatedClosingDate: { type: 'dateonly' },
            solarFundingDate: { type: 'dateonly' },
            solarCloserName: { type: 'string' },
            solarCloserPhone: { type: 'string', validate: PHONE_NUMBER_REGEXP },
            solarCloserEmail: { type: 'string', validate: EMAIL_REGEXP },
            solarSecondMortgageClosingPackageConfirmedAt: { type: 'datetime' },
            solarWireInstructionsVerifiedAt: { type: 'datetime' },

            collateralSentToCustodianAt: { type: 'datetime' },
            collateralCustodian: { type: 'string', options: ['Bank of the Sierra', 'US Bank', 'South Star Bank'] },
            collateralCustodianTrackingNumber: { type: 'string' },

            secondMortgageAllongeRequired: { type: 'boolean' },

            csPortalLoanAttachmentsCount: { type: 'integer' },
            csPortalAlwaysSync: { type: 'boolean' },

            eepFhaTotalMonthlyPayment: { type: 'decimal' },
            eepPandIPayment: { type: 'decimal' },
            eepTotalMonthlyPayment: { type: 'decimal' }, // this is the amount of the lease payment made by the program participant
            eepCurrentPrincipalBalance: { type: 'decimal' },
            eepProgramType: { type: 'string', label: 'EEP Program Type', options: ['Homebuyer Agreement', 'Long-term Purchase Agreement'] },
            eepProgramRate: { type: 'decimal' },
            eepEscrowPayment: { type: 'decimal' },
            eepMiscEscrow: { type: 'decimal' },
            eepPmiPayment: { type: 'decimal' },
            eepLeaseFirstPaymentDueDate: { type: 'dateonly' },
            eepUnitProximityReportStatus: { type: 'string', options: ['Eligible', 'Needs Review', 'Not Eligible'] },

            paymentChangeOldPrincipalAndInterest: { type: 'decimal' },
            paymentChangeNewPrincipalAndInterest: { type: 'decimal' },
            paymentChangeOldTaxesAndInsurance: { type: 'decimal' },
            paymentChangeNewTaxesAndInsurance: { type: 'decimal' },
            paymentChangeOldMortgageInsurance: { type: 'decimal' },
            paymentChangeNewMortgageInsurance: { type: 'decimal' },
            paymentChangeOldHomeownersAssociation: { type: 'decimal' },
            paymentChangeNewHomeownersAssociation: { type: 'decimal' },
            paymentChangeOldProgramMaintenanceFee: { type: 'decimal' },
            paymentChangeNewProgramMaintenanceFee: { type: 'decimal' },
            paymentChangeNewPaymentEffectiveDate: { type: 'dateonly' },
            paymentChangeRefundEnclosed: { type: 'boolean' },
            
            processCsportalEepLoanThroughBlueWater: { type: 'boolean' },

            occupancyType: { type: 'string', options: ['Owner Occupied', 'Non-Occupant Borrower', 'Rent', 'Second Home', 'Investment Property'] },
            bankruptcyFlag: { type: 'string', options: ['C', 'A'] },

            loanSale: {type: 'LoanSale'},
            secondMortgageDelinquencyStatus: { type: 'string', options: ['0-29', '30-59', '60-89', '90+'] },
            secondMortgageInvestor: { type: 'Investor' },
            secondMortgageLienPosition: { type: 'string' },
            secondMortgageOwnershipStatus: { type: 'string', options: SECOND_MORTGAGE_OWNERSHIP_STATUSES },
            secondMortgageSaleStatus: { type: 'string', options: ['Not Sold', 'Pending Sale', 'Sold', 'Paid In Full', 'Repurchased'] }, // this field is deprecated in favor of secondMortgageOwnershipStatus
            secondMortgageSettledStatus: { type: 'string', options: ['Settled', 'Unsettled'] }, // this field is deprecated in favor of secondMortgageOwnershipStatus
            secondMortgageSaleId: { type: 'string' }, // managed via bulk updater

            purchaseAdvicePurchaseDate: { type: 'dateonly' },
            purchaseAdviceNextPaymentAmount: { type: 'decimal', format: 'dollars', commas: true },
            purchaseAdviceNextPaymentDate: { type: 'dateonly' },
            purchaseAdviceFirstPaymentToMountainWestDate: { type: 'dateonly' },

            // Removing these for now, but leaving comments to show that we have this data for loans batched in May, June, and July of 2025
            // purchaseAdviceNextPaymentPrincipalAmount: { type: 'decimal', format: 'dollars', commas: true },
            // purchaseAdviceNextPaymentMortgageInsuranceAmount: { type: 'decimal', format: 'dollars', commas: true },
            // purchaseAdviceNextPaymentEscrowAmount: { type: 'decimal', format: 'dollars', commas: true },

            purchaseAdviceRolloverFeesAmount: { type: 'decimal', format: 'dollars', commas: true },
            purchaseAdviceRolloverFeesPercent: { type: 'decimal', format: 'percent' },
            purchaseAdvicePerDiemInterestToCorrespondent: { type: 'decimal', format: 'dollars', commas: true },
            purchaseAdvicePerDiemInterestToInvestor: { type: 'decimal', format: 'dollars', commas: true },
            purchaseAdvicePerDiemInterestDays: { type: 'integer' },
            purchaseAdviceAdminFee: { type: 'decimal', format: 'dollars', commas: true },
            purchaseAdviceImpounds: { type: 'decimal', format: 'dollars', commas: true },
            purchaseAdviceOtherInvestorStipulations: { type: 'decimal', format: 'dollars', commas: true },
            purchaseAdviceAdditionalNotes: { type: 'string' },
            purchaseAdviceTotalWireAmount: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            purchaseAdvicePredictedSalePrice: { type: 'decimal', format: 'percent', decimalPlaces: 3 },
            purchaseAdviceHaircutAmount: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            taxIdFor1003: { type: 'string' },
            correspondentWarehouseBank: { type: 'CorrespondentWarehouseBank' },

            reconciliationBatch: { type: 'ReconciliationBatch' },

            propertyCoordinates: { type: 'any' },
            propertyCoordinatesRequestError: { type: 'string' },
            propertyAddressCurrentGeocodeHash: { type: 'string' },
            propertyAddressHash: { type: 'string' },
            
            trades: { type: 'Trade[]' },
            warehouseBatch: { type: 'WarehouseBatch' },

            secondMortgageRepurchaseStatus: { type: 'string', options: [
                // In the future, we may add statuses to track AH repurchasing a second mortgage from an investor
                'Pending Review',
                'Demand Letter Sent - Funds Not Received',
                'Funds Received - Pending Service Transfer',
                'Repurchase Complete',
                'Discount Option Taken',
                'Reinstated',
                'Paid In Full',
            ] },
            secondMortgageRepurchasePendingReviewAt: { type: 'datetime' },
            secondMortgageRepurchaseDemandLetterSentAt: { type: 'datetime' },
            secondMortgageRepurchaseFundsReceivedAt: { type: 'datetime' },
            secondMortgageRepurchaseDiscountOptionTaken: { type: 'boolean' },
            secondMortgageRepurchaseServiceTransferCompletedAt: { type: 'datetime' },
            secondMortgageRepurchaseReinstatedAt: { type: 'datetime' },
            secondMortgageRepurchasePaidInFullAt: { type: 'datetime' },
            secondMortgageRepurchaseLateFee: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            secondMortgageRepurchaseUnappliedFunds: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            secondMortgageRepurchaseUnpaidFees: { type: 'decimal', format: 'dollars', commas: true, dedecimalPlacescimals: 2 },
            secondMortgageRepurchaseInterest: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            secondMortgageRepurchaseGoodThroughDate: { type: 'dateonly' },
            secondMortgageRepurchaseDefaultPaymentDate: { type: 'dateonly' },

            applicationDate: { type: 'dateonly' },
            fundingDate: { type: 'dateonly' },
            temporaryBuydown: { type: 'boolean', tristate: true },
            firstMortgagePAndIPayment: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            firstMortgageLastPaymentMadeDate: { type: 'dateonly' },

            isEmployeeLoan: { type: 'boolean' },
            
            uncategorizedDocumentCount: { type: 'number' },
            parentCorrespondent: { type: 'Correspondent' },

            fivePercentDpaAuditRequired: { type: 'boolean', label: '5% DPA Audit Required' },
            fivePercentDpaAuditCompleted: { type: 'boolean', label: '5% DPA Audit Completed' },

            eepSupportSpecialist: { type: 'User', label: 'EEP Support Specialist' },
            eepProcessor: { type: 'User', label: 'EEP Processor' },
            eepUnderwriter: { type: 'User', label: 'EEP Underwriter' },
            eepCloser: { type: 'User', label: 'EEP Closer' },
            eepQualifyingDocument: { type: 'string', options: [
                'Bank Statement',
                'P&L Statement',
                'Paystubs/W2s',
                'Tax Returns',
            ] },
            eepFileReviewedAt: { type: 'datetime' },
            submittedToUnderwritingAt: { type: 'datetime' },
            underwritingSentApprovalDocumentAt: { type: 'datetime' },
            approvedAt: { type: 'datetime' },
            specifyProperty: { type: 'boolean' },
            eepProcessingEntries: { type: 'EepProcessingEntry[]', label: 'EEP Processing Entries' },
            clearedToCloseDate: { type: 'datetime' },
            eepClosingPackageSentToTitleAt: { type: 'datetime', label: 'EEP Closing Package Sent To Title' },

            originatorOrganizationNmlsr: { type: 'string', label: 'Originator Organization NMLSR' },
            originatorOrganizationStateLicenseId: { type: 'string' },
            originatorNmlsr: { type: 'string', label: 'Originator NMLSR' },
            originatorStateLicenseId: { type: 'string' },

            postPurchaseConditionsContactName: { type: 'string' },
            postPurchaseConditionsContactEmail: { type: 'string' },
            postPurchaseConditionsContactPhone: { type: 'phone' },

            propertyInsurancePurchaser: { type: 'string', options: ['Lender', 'Arrive Home']},
            insuredParty: { type: 'string', options: ['Tule River', 'Homebuyer'] },

            utilitiesOwnerRequired: { type: 'boolean' },
            waterSewerCompany: { type: 'string' },
            gasCompany: { type: 'string' },
            electricCompany: { type: 'string' },

            hasHoa: { type: 'boolean' },
            hoaName: { type: 'string' },
            hoaEmail: { type: 'string' },
            hoaPhone: { type: 'string' },
            hoaWebsite: { type: 'string' },
            hoaContactName: { type: 'string' },

            sellerCredit: { type: 'decimal', format: 'dollars' },
            realEstateTaxes: { type: 'decimal', format: 'dollars' },
            hazardInsurance: { type: 'decimal', format: 'dollars' },
            mortgageInsurance: { type: 'decimal', format: 'dollars' },
            hoaDues: { type: 'decimal', format: 'dollars' },
            titleInsurer: { type: 'string' },

            hedgeException: { type: 'boolean' },

            secondNoteHomegenAllongeSigner: { type: 'User' },
            secondNoteTrfaAllongeSigner: { type: 'User' },

            constructionType: { type: 'string', options: [
                'Brick Veneer',
                'Joisted Masonry',
                'Wood Frame',
                'Unknown',
            ]},
            squareFootage: { type: 'integer' },
            yearBuilt: { type: 'integer' },
            yearOfLatestPlumbingRenovation: { type: 'integer' },
            inFloodZone: { type: 'boolean' },
            roofIsOlderThan15Years: { type: 'boolean' },
            electricalIsOnBreaker: { type: 'boolean' },
            hasSwimmingPool: { type: 'boolean' },
            isSection8Housing: { type: 'boolean' },
            propertyInsuranceCoverageAmount: { type: 'decimal', format: 'dollars', decimalPlaces: 0 },
            needsLossOfRentCoverage: { type: 'boolean' },
            monthlyRent: { type: 'decimal', format: 'dollars', decimalPlaces: 0 },
            lenderIsLossPayee: { type: 'boolean' },
            lenderContact: { type: 'User' },
            additionalLenderIsLossPayee: { type: 'boolean' },
            additionalInsuredName: { type: 'string' },
            additionalInsuredAddress: { type: 'string' },
            additionalInsuredCity: { type: 'string' },
            additionalInsuredState: { type: 'string' },
            additionalInsuredZip: { type: 'string' },
            additionalLenderContactName: { type: 'string' },
            additionalLenderContactPhone: { type: 'string' },
            additionalLenderContactEmail: { type: 'string' },
            dwellingPolicyType: { type: 'string', options: [
                'DP3',
                'HO3',
            ]},
            tuleRiverListedAsAdditionalInsured: { type: 'boolean' },
            tuleRiverListedAsPrimaryInsured: { type: 'boolean' },
            insuranceBrokerName: { type: 'string' },
            insuranceBrokerPhone: { type: 'string' },
            insuranceBrokerAddress: { type: 'string' },
            insuranceProviderName: { type: 'string' }, // Is provider the same as carrier?
            insuranceCarrierPhone: { type: 'string' },
            insuranceCarrierAddress: { type: 'string' },
            insuranceCarrierCreditRating: { type: 'string' },
            propertyReplaceableCostEstimate: { type: 'decimal', format: 'dollars', decimalPlaces: 0 },
            propertyInsuranceDeductibleAmount: { type: 'decimal', format: 'dollars', decimalPlaces: 0 },
            propertyInsurancePremiumAmount: { type: 'decimal', format: 'dollars', decimalPlaces: 0 },
            floodInsuranceMarket: { type: 'string', options: [
                'NFIP', // National Flood Insurance Program
                'Private',
            ]},
            floodInsuranceDedctibleAmount: { type: 'decimal', format: 'dollars', decimalPlaces: 0 },
            liabilityInsuranceCoverageAmount: { type: 'decimal', format: 'dollars', decimalPlaces: 0 },
            liabilityInsuranceDeductibleAmount: { type: 'decimal', format: 'dollars', decimalPlaces: 0 },
            liabilityInsurancePremiumAmount: { type: 'decimal', format: 'dollars', decimalPlaces: 0 },
            currentMortgagee: { type: 'string' },
            roofAgeYears: { type: 'integer' },
            propertyCondition: { type: 'string', options: [
                'C1',
                'C2',
                'C3',
                'C4',
                'C5',
            ]},
            wireReceived: { type: 'boolean' }, // What is this wire for?
            wireRecipient: { type: 'string', options: [
                'Title Company',
                'Tule River',
            ]},
            titleFunded: { type: 'boolean' },
            lenderIncomplete: { type: 'boolean' },
            homeownersInsuranceRequestedDate: { type: 'dateonly' },
            homeownersInsuranceReceivedDate: { type: 'dateonly' },
            insurancePolicyNumber: { type: 'string' },
            insuranceProcurement: { type: 'string', options: [
                'Hazard & Flood Steadily',
                'AH Lockton',
                'AH Steadily',
                'Hazard Lockton and Flood Steadily',
                'Lender TR Insured',
                'Hazard Flood Steadily and Wind',
                'Lender HB Insured',
            ]},
            servicingAssignedTo: { type: 'User' },
            allongeWarehouseShipDate: { type: 'dateonly' },
            allongeWarehouseTrackingNumber: { type: 'string' },

            closingPackageRequestedAt: { type: 'datetime' },
            
            hasUnclearedPostPurchaseConditions: { type: 'boolean' },
            sentAllClosingRequestConditionsClearedNotificationAt: { type: 'datetime' },
            sentClearToCloseNotificationAt: { type: 'datetime' },
        },
        indexes: [
            { fields: ['postgresId'] },
            { fields: ['blueWaterId'] },
            { fields: ['ahLoanNumber'], unique: true },
            { fields: ['bsiLoanNumber'] },
            { fields: ['firstMortgageMinNumber'] },
            { fields: ['secondMortgageMinNumber'] },
            { fields: ['lenderLoanNumber'] },
            { fields: ['correspondentId'] },
            { fields: ['parentCorrespondentId'] },
            { fields: ['status'] },
            { fields: ['tradeIds'] },
            { fields: ['eepUnitProximityReportStatus'] },
            { fields: ['uncategorizedDocumentCount'] },
            { fields: ['loanCallLogs.calledAt'] },
            { fields: ['registeredAt'] },
            { fields: ['borrowers.ssn'] },
            { fields: ['productType', 'purchasedAt'] },
            { fields: ['firstMortgageOwnershipStatus'] },
        ]
    },

    Borrower: {
        nested: true,
        fields: {
            _key: { type: 'string' },
            _position: { type: 'number' },
            firstName: { type: 'string', sensitive: true },
            lastName: { type: 'string', sensitive: true },
            middleName: { type: 'string', sensitive: true },
            suffix: { type: 'string', sensitive: true },
            email: { type: 'string', sensitive: true, validate: EMAIL_REGEXP },

            primaryPhoneNumber: { type: 'string', sensitive: true, validate: PHONE_NUMBER_REGEXP },
            homePhoneNumber: { type: 'string', sensitive: true, validate: PHONE_NUMBER_REGEXP },
            workPhoneNumber: { type: 'string', sensitive: true, validate: PHONE_NUMBER_REGEXP },

            currentAddress: { type: 'string', sensitive: true },
            currentCity: { type: 'string', sensitive: true },
            currentState: { type: 'string', sensitive: true, options: US_STATE_CODES },
            currentZip: { type: 'string', sensitive: true },

            ssn: { type: 'string', sensitive: true, label: 'SSN', validate: /^\d\d\d-\d\d-\d\d\d\d$|^\d{9}$/ },
            dateOfBirth: { type: 'dateonly', sensitive: true },
            sex: { type: 'string', options: ['Male', 'Female', 'Declined To Answer'], sensitive: true },
            race: { type: 'string', sensitive: true },
            ethnicity: { type: 'string', sensitive: true },
            monthlyIncome: { type: 'decimal', format: 'dollars', commas: true, sensitive: true },
            fico: { type: 'integer', sensitive: true, label: 'FICO', gte: 1, lte: 850 },
            dti: { type: 'decimal', sensitive: true }, // TODO: this isn't really used in the current process
            employerName: { type: 'string' },
            employerAddress: { type: 'string' },
            employerCity: { type: 'string' },
            employerState: { type: 'string' },
            employerZip: { type: 'string' },
            employerPhoneNumber: { type: 'phone' },
            position: { type: 'string' },
            yearsWorked: { type: 'number' },
            maritalStatus: { type: 'string', options: ['Unmarried', 'Married', 'Separated'] },

            personalReferenceName: { type: 'string' },
            personalReferenceRelationship: { type: 'string' },
            personalReferencePhone: { type: 'string' },
        }
    },

    EepProcessingEntry: {
        nested: true,
        fields: {
            _key: { type: 'string' },
            _position: { type: 'number' },
            at: { type: 'datetime' },
            user: { type: 'User' },
            newProcessingStatus: { type: 'string', options: EEP_PROCESSING_STATUS_OPTIONS },
        },
    },

    LoanSnapshot: {
        fields: {
            _id: { type: 'id' },
            loan: { type: 'Loan' },
            snapshotDate: { type: 'dateonly' },

            productType: { type: 'string', options: ['DPA', 'EEP', 'White Label', 'Solar Program'] },
            dpaRepaymentType: { type: 'string', options: ['Repayable', 'Forgivable'], label: 'DPA Repayment Type' },
            status: { type: 'string', options: LOAN_STATUS_OPTIONS },
            registeredAt: { type: 'datetime' },
            dpaAmount: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 0  },

            secondMortgageOwnershipStatus: { type: 'string', options: SECOND_MORTGAGE_OWNERSHIP_STATUSES },
            secondMortgageSettledStatus: { type: 'string', options: ['Settled', 'Unsettled'] }, // this field is deprecated in favor of secondMortgageOwnershipStatus
            secondMortgageSaleStatus: { type: 'string', options: ['Not Sold', 'Pending Sale', 'Sold', 'Paid In Full', 'Repurchased'] }, // this field is deprecated in favor of secondMortgageOwnershipStatus
            warehousePrincipalBalance: { type: 'decimal', format: 'dollars', commas: true },
            secondMortgageUpb: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            secondMortgageDelinquencyStatus: { type: 'string', options: ['0-29', '30-59', '60-89', '90+'] },
        },
        indexes: [
            { fields: ['snapshotDate', 'loanId'], unique: true },
            { fields: ['loanId', 'snapshotDate'] },
        ]
    },

    ScorecardSnapshot: {
        fields: {
            _id: { type: 'id' },
            correspondent: { type: 'Correspondent' },
            snapshotDate: { type: 'dateonly' },

            totalServicingUnits: { type: 'number', commas: true },
            totalServicingVolume: { type: 'number', format: 'dollars', commas: true, decimalPlaces: 0 },

            servicingUnitsDelinquent30To59: { type: 'number', commas: true },
            servicingUnitsDelinquent60To89: { type: 'number', commas: true },
            servicingUnitsDelinquent90plus: { type: 'number', commas: true },
            servicingVolumeDelinquent30To59: { type: 'number', format: 'dollars', commas: true, decimalPlaces: 0 },
            servicingVolumeDelinquent60To89: { type: 'number', format: 'dollars', commas: true, decimalPlaces: 0 },
            servicingVolumeDelinquent90plus: { type: 'number', format: 'dollars', commas: true, decimalPlaces: 0 },

            dpaRepurchase35Units: { type: 'number', format: 'percent' },
            dpaRepurchase50Units: { type: 'number', format: 'percent' },
            dpaRepurchase35Volume: { type: 'number', format: 'dollars', commas: true, decimalPlaces: 0 },
            dpaRepurchase50Volume: { type: 'number', format: 'dollars', commas: true, decimalPlaces: 0 },
            wlRepurchase35Units: { type: 'number', format: 'percent' },
            wlRepurchase50Units: { type: 'number', format: 'percent' },
            wlRepurchase35Volume: { type: 'number', format: 'dollars', commas: true, decimalPlaces: 0 },
            wlRepurchase50Volume: { type: 'number', format: 'dollars', commas: true, decimalPlaces: 0 },      

            delinquency30To59UnitsByFico600to639: { type: 'number', commas: true },
            delinquency60To89UnitsByFico600to639: { type: 'number', commas: true },
            delinquency90plusUnitsByFico600to639: { type: 'number', commas: true },
            delinquency30To59VolumeByFico600to639: { type: 'number', format: 'dollars', commas: true, decimalPlaces: 0 },
            delinquency60To89VolumeByFico600to639: { type: 'number', format: 'dollars', commas: true, decimalPlaces: 0 },
            delinquency90plusVolumeByFico600to639: { type: 'number', format: 'dollars', commas: true, decimalPlaces: 0 },

            delinquency30To59UnitsByFico640to679: { type: 'number', commas: true },
            delinquency60To89UnitsByFico640to679: { type: 'number', commas: true },
            delinquency90plusUnitsByFico640to679: { type: 'number', commas: true },
            delinquency30To59VolumeByFico640to679: { type: 'number', format: 'dollars', commas: true, decimalPlaces: 0 },
            delinquency60To89VolumeByFico640to679: { type: 'number', format: 'dollars', commas: true, decimalPlaces: 0 },
            delinquency90plusVolumeByFico640to679: { type: 'number', format: 'dollars', commas: true, decimalPlaces: 0 },

            delinquency30To59UnitsByFico680to699: { type: 'number', commas: true },
            delinquency60To89UnitsByFico680to699: { type: 'number', commas: true },
            delinquency90plusUnitsByFico680to699: { type: 'number', commas: true },
            delinquency30To59VolumeByFico680to699: { type: 'number', format: 'dollars', commas: true, decimalPlaces: 0 },
            delinquency60To89VolumeByFico680to699: { type: 'number', format: 'dollars', commas: true, decimalPlaces: 0 },
            delinquency90plusVolumeByFico680to699: { type: 'number', format: 'dollars', commas: true, decimalPlaces: 0 },

            delinquency30To59UnitsByFico700Plus: { type: 'number', commas: true },
            delinquency60To89UnitsByFico700Plus: { type: 'number', commas: true },
            delinquency90plusUnitsByFico700Plus: { type: 'number', commas: true },
            delinquency30To59VolumeByFico700Plus: { type: 'number', format: 'dollars', commas: true, decimalPlaces: 0 },
            delinquency60To89VolumeByFico700Plus: { type: 'number', format: 'dollars', commas: true, decimalPlaces: 0 },
            delinquency90plusVolumeByFico700Plus: { type: 'number', format: 'dollars', commas: true, decimalPlaces: 0 },

            delinquency30To59UnitsByDti0to39: { type: 'number', commas: true },
            delinquency60To89UnitsByDti0to39: { type: 'number', commas: true },
            delinquency90plusUnitsByDti0to39: { type: 'number', commas: true },
            delinquency30To59VolumeByDti0to39: { type: 'number', format: 'dollars', commas: true, decimalPlaces: 0 },
            delinquency60To89VolumeByDti0to39: { type: 'number', format: 'dollars', commas: true, decimalPlaces: 0 },
            delinquency90plusVolumeByDti0to39: { type: 'number', format: 'dollars', commas: true, decimalPlaces: 0 },

            delinquency30To59UnitsByDti40to44: { type: 'number', commas: true },
            delinquency60To89UnitsByDti40to44: { type: 'number', commas: true },
            delinquency90plusUnitsByDti40to44: { type: 'number', commas: true },
            delinquency30To59VolumeByDti40to44: { type: 'number', format: 'dollars', commas: true, decimalPlaces: 0 },
            delinquency60To89VolumeByDti40to44: { type: 'number', format: 'dollars', commas: true, decimalPlaces: 0 },
            delinquency90plusVolumeByDti40to44: { type: 'number', format: 'dollars', commas: true, decimalPlaces: 0 },

            delinquency30To59UnitsByDti45to49: { type: 'number', commas: true },
            delinquency60To89UnitsByDti45to49: { type: 'number', commas: true },
            delinquency90plusUnitsByDti45to49: { type: 'number', commas: true },
            delinquency30To59VolumeByDti45to49: { type: 'number', format: 'dollars', commas: true, decimalPlaces: 0 },
            delinquency60To89VolumeByDti45to49: { type: 'number', format: 'dollars', commas: true, decimalPlaces: 0 },
            delinquency90plusVolumeByDti45to49: { type: 'number', format: 'dollars', commas: true, decimalPlaces: 0 },

            delinquency30To59UnitsByDti50Plus: { type: 'number', commas: true },
            delinquency60To89UnitsByDti50Plus: { type: 'number', commas: true },
            delinquency90plusUnitsByDti50Plus: { type: 'number', commas: true },
            delinquency30To59VolumeByDti50Plus: { type: 'number', format: 'dollars', commas: true, decimalPlaces: 0 },
            delinquency60To89VolumeByDti50Plus: { type: 'number', format: 'dollars', commas: true, decimalPlaces: 0 },
            delinquency90plusVolumeByDti50Plus: { type: 'number', format: 'dollars', commas: true, decimalPlaces: 0 },

            delinquency35DpaUnits30To59: { type: 'number', commas: true },
            delinquency35DpaUnits60To89: { type: 'number', commas: true },
            delinquency35DpaUnits90plus: { type: 'number', commas: true },
            delinquency35DpaVolume30To59: { type: 'number', format: 'dollars', commas: true, decimalPlaces: 0 },
            delinquency35DpaVolume60To89: { type: 'number', format: 'dollars', commas: true, decimalPlaces: 0 },
            delinquency35DpaVolume90plus: { type: 'number', format: 'dollars', commas: true, decimalPlaces: 0 },


            delinquency50DpaUnits30To59: { type: 'number', commas: true },
            delinquency50DpaUnits60To89: { type: 'number', commas: true },
            delinquency50DpaUnits90plus: { type: 'number', commas: true },
            delinquency50DpaVolume30To59: { type: 'number', format: 'dollars', commas: true, decimalPlaces: 0 },
            delinquency50DpaVolume60To89: { type: 'number', format: 'dollars', commas: true, decimalPlaces: 0 },
            delinquency50DpaVolume90plus: { type: 'number', format: 'dollars', commas: true, decimalPlaces: 0 },

        },
        indexes: [
            { fields: ['snapshotDate', 'correspondentId'], unique: true },
            { fields: ['correspondentId', 'snapshotDate'] },
        ]
    },


    BotsReportRecord: {
        fields: {
            _id: { type: 'id' },
            botsLoanNumber: { type: 'string' },
            loan: { type: 'Loan' },
            rawData: { type: 'any' },
            createdAt: { type: 'datetime' },
        },
        indexes: [
            { fields: ['botsLoanNumber'] },
            { fields: ['loanId'] },
        ]
    },

    Investor: {
        fields: {
            _id: { type: 'id' },
            name: { type: 'string' },
            isSecuritizationInvestor: { type: 'boolean' },
        }
    },

    WarehouseBank: {
        fields: {
            _id: { type: 'id' },
            name: { type: 'string' },
        }
    },

    CorrespondentWarehouseBank: {
        fields: {
            _id: { type: 'id' },
            correspondent: { type: 'Correspondent' },
            name: { type: 'string' },
            wireRoutingNumber: { type: 'string', sensitive: true },
            wireAccountNumber: { type: 'string', sensitive: true },
        }
    },

    LoanComment: {
        nested: true,
        fields: {
            _key: { type: 'string' },
            _position: { type: 'number' },
            user: { type: 'User' },
            at: { type: 'datetime' },
            text: { type: 'string', lines: 4 },
            postgresId: { type: 'integer' },
        }
    },

    LoanCallLog: {
        nested: true,
        fields: {
            _key: { type: 'string' },
            _position: { type: 'number' },
            callType: { type: 'string', options: ['Inbound', 'Outbound'] },
            callerId: { type: 'string', options: ['Borrower', 'Co-Borrower', 'Authorized 3rd Party'] },
            servicingSpecialist: { type: 'User' },
            callDisposition: { type: 'string', options: ['Contact', 'No Contact', 'Wrong Number', 'Message Left'] },
            reasonForDefault: { type: 'string' },
            callOutcome: { type: 'string', options: ['Promise to Pay', 'Partial Reinstatement', 'Full Reinstatement', 'Repayment Plan'] },
            nextAction: { type: 'string' },
            user: { type: 'User' },
            calledAt: { type: 'datetime' },
            text: { type: 'string', lines: 4 },
        },
    },

    LoanPriceAdjustment: {
        nested: true,
        fields: {
            _key: { type: 'string' },
            _position: { type: 'number' },
            user: { type: 'User' },
            at: { type: 'datetime' },
            label: { type: 'string' },
            amount: { type: 'decimal' },
            isFromRateSheet: { type: 'boolean' },
        }
    },

    Exception: {
        nested: true,
        fields: {
            _key: { type: 'string' },
            _position: { type: 'number' },
            createdBy: { type: 'User', required: true },
            createdAt: { type: 'datetime', required: true },
            exceptionType: {
                type: 'string',
                options: [
                    'Credit',
                    'Income',
                    'DTI',
                    'Housing Payment',
                    'Other',
                ],
                required: true,
            },
            description: { type: 'string', lines: 3, required: true },
        }
    },

    UsfReportRecord: {
        fields: {
            _id: { type: 'id' },
            usfLoanNumber: { type: 'string' },
            loan: { type: 'Loan' },
            rawData: { type: 'any' },
            createdAt: { type: 'datetime' },
        },
        indexes: [
            { fields: ['usfLoanNumber'] },
            { fields: ['loanId'] },
        ]
    },    

    WorkflowIssue: {
        fields: {
            _id: { type: 'id' },
            loan: { type: 'Loan' },
            loanStatus: { type: 'string', options: LOAN_STATUS_OPTIONS },
            createdAt: { type: 'datetime' },
            createdBy: { type: 'User' },
            assignedTo: { type: 'User' },
            resolvedAt: { type: 'datetime' },
            resolvedBy: { type: 'User' },
            description: { type: 'string', lines: 4, required: true },
            automationType: { type: 'string', options: AUTOMATION_TYPE_OPTIONS }
        },
        indexes: [
            { fields: ['loanId'] },
        ]
    },

    LoanCondition: {
        fields: {
            _id: { type: 'id' },
            loan: { type: 'Loan' },
            conditionSource: { type: 'string', options: ['Blue Water', 'Internal'] },
            conditionType: {
                type: 'string',
                options: [
                    'EEP Tenant (Prior To Close)',
                    'EEP Entity (Prior To Close)',
                    'EEP Closing Request',
                    'EEP Closing',
                    'EEP Tenant (Purchase Clearing)',
                    'EEP Entity (Purchase Clearing)',
                    'Missing Document (Purchase Clearing)',
                    'Correction Needed (Purchase Clearing)',
                    'Collateral Not Dry',
                    'Missing Document (Post Purchase)',
                    'Correction Needed (Post Purchase)',
                ],
                required: true,
            },
            documentTypes: { type: 'string[]', options: LOAN_DOCUMENT_TYPES },
            documentName: { type: 'string' },
            fieldName: { type: 'string' },
            blueWaterFieldValueId: { type: 'integer' },
            unconfirmedFieldValue: { type: 'string' },
            confirmedFieldValue: { type: 'string' },
            createdAt: { type: 'datetime' },
            createdBy: { type: 'User' },
            submittedAt: { type: 'datetime' },
            submittedBy: { type: 'User' },
            insufficientAt: { type: 'datetime' },
            insufficientBy: { type: 'User' },
            clearedAt: { type: 'datetime' },
            clearedBy: { type: 'User' },
            description: { type: 'string', lines: 4 },
        },
        indexes: [
            { fields: ['loanId'] },
        ]
    },

    LoanConditionEvent: {
        fields: {
            _id: { type: 'id' },
            loanCondition: { type: 'LoanCondition' },
            eventType: { type: 'string', options: ['Created', 'Submitted', 'Insufficient', 'Cleared', 'Comment', 'Internal Comment'] },
            at: { type: 'datetime' },
            by: { type: 'User' },
            text: { type: 'string', lines: 4 },
        },
        indexes: [
            { fields: ['loanConditionId', 'at'] },
        ],
    },

    PostgresRawLoanRecord: {
        fields: {
            _id: { type: 'id' },
            loan: { type: 'Loan' },
            createdAt: { type: 'datetime' },
            updatedAt: { type: 'datetime' },
            columns: { type: 'any' },
            values: { type: 'any' },
            usedFields: { type: 'any' },
        },
        indexes: [
            { fields: ['loanId'], unique: true },
        ],
    },

    PostgresLoanValue: {
        fields: {
            _id: { type: 'id' },
            postgresId: { type: 'integer' },
            loanId: { type: 'integer' },
            loanFieldId: { type: 'integer' },
            value: { type: 'string' },
            createdAt: { type: 'datetime' },
            updatedAt: { type: 'datetime' },
            fieldName: { type: 'string' },
        },
        indexes: [
            { fields: ['postgresId'], unique: true },
            { fields: ['loanId', 'postgresId'] },
        ]
    },

    CsPortalFieldEntry: {
        fields: {
            _id: { type: 'id' },
            fieldType: { type: 'string', options: ['column', 'value'] },
            name: { type: 'string' },
            postgresIds: { type: 'string[]' },
            exampleValue1: { type: 'string' },
            exampleValue2: { type: 'string' },
            exampleValue3: { type: 'string' },
        },
        indexes: [
            { fields: ['fieldType', 'name'], unique: true },
        ]
    },

    LoanDocument: {
        fields: {
            _id: { type: 'id' },
            postgresId: { type: 'integer' },
            postgresName: { type: 'string' },
            loan: { type: 'Loan' },
            name: { type: 'string' },
            description: { type: 'string' },
            contentType: { type: 'string' },
            size: { type: 'integer', commas: true },
            pageCount: { type: 'integer', commas: true },
            s3Key: { type: 'string' },
            s3MultipartUploadId: { type: 'string' },
            sessionToken: { type: 'string' },
            uploadedAt: { type: 'datetime' },
            uploadedBy: { type: 'User' },
            archivedAt: { type: 'datetime' },
            archivedBy: { type: 'User' },
            uploadedToBlueWaterAt: { type: 'datetime' },
            blueWaterTransferMilliseconds: { type: 'number' },
            blueWaterFinalVersion: { type: 'boolean', tristate: true },
            loanCondition: { type: 'LoanCondition' },
            loanConditionEvent: { type: 'LoanConditionEvent' },
            documentType: {
                type: 'string',
                options: LOAN_DOCUMENT_TYPES,
            },
            eepConditionTemplateName: { type: 'string' },
            hash: { type: 'string' },
        },
        indexes: [
            { fields: ['loanId', 'documentType'] },
            { fields: ['postgresId'], unique: true, partial: { postgresId: { $type: 'number' } } },
            { fields: ['loanConditionId'] },
            { fields: ['loanConditionEventId'] },
            { fields: ['uploadedToBlueWaterAt'] }
        ]
    },

    LoanDocumentUploadPart: {
        fields: {
            _id: { type: 'id' },
            loanDocument: { type: 'LoanDocument' },
            partNumber: { type: 'integer' },
            etag: { type: 'string' },
        },
        indexes: [
            { fields: ['loanDocumentId', 'partNumber'], unique: true },
        ]
    },

    BsiBoardingBatch: {
        fields: {
            _id: { type: 'id' },
            postgresId: { type: 'integer' },
            createdAt: { type: 'datetime' },
            date: { type: 'dateonly' },
            productType: { type: 'string', options: ['DPA', 'White Label'] },
            correspondent: { type: 'Correspondent' },
            status: { type: 'string', options: ['Failed', 'Pending', 'Uploaded'] },
            loans: { type: 'Loan[]' },
            uploadedAt: { type: 'datetime' },
            s3Key: { type: 'string', label: 'S3 Key' },
            sftpPath: { type: 'string' },
        },
        indexes: [
            { fields: ['loanIds'] },
        ]
    },

    BsiBoardingBatchLoan: {
        fields: {
            _id: { type: 'id' },
            bsiBoardingBatch: { type: 'BsiBoardingBatch' },
            loan: { type: 'Loan' },
            uploadedAt: { type: 'datetime' },
        },
        indexes: [
            { fields: ['uploadedAt' ] },
        ]
    },

    BsiServicingRecord: {
        fields: {
            _id: { type: 'id' },
            collateralDescription: { type: 'string' }, // this is generally the ahLoanNumber, but that is not true for some earlier records
            date: { type: 'dateonly' },
            rawData: { type: 'any' },
        },
        indexes: [
            { fields: ['collateralDescription', 'date'] },
            { fields: ['date'] },
        ]
    },

    ElevatoLogin: { 
        fields: {
            _id: { type: 'id' },
            uuid: { type: 'string' },
            user: { type: 'User' },
            target: { type: 'string' },
            createdAt: { type: 'datetime' },
            usedAt: { type: 'datetime' },
        },
        indexes: [
            { fields: ['uuid'], unique: true },
        ]
    },

    BlueWaterDeal: {
        fields: {
            _id: { type: 'id' },
            blueWaterId: { type: 'integer' },
            name: { type: 'string' },
        },
        indexes: [
            { fields: ['blueWaterId'], unique: true },
        ]
    },

    BlueWaterRawLoanRecord: {
        fields: {
            _id: { type: 'id' },
            loan: { type: 'Loan' },
            createdAt: { type: 'datetime' },
            updatedAt: { type: 'datetime' },
            consolidatedFields: { type: 'any' },
            usedFields: { type: 'any' },
        },
        indexes: [
            { fields: ['loanId'], unique: true },
        ],
    },

    LoanFieldLookupEntry: {
        fields: {
            _id: { type: 'id' },
            indexedAt: { type: 'datetime' },
            fieldLabel: { type: 'string' },
            fieldName: { type: 'string' },
            screenName: { type: 'string' },
            screenTitle: { type: 'string' },
        },
        indexes: [
            { fields: ['fieldLabel', 'screenName'], unique: true },
        ]
    },

    ProjectConfig: {
        fields: {
            _id: { type: 'id' },
            elevatoLoginKey: { type: 'string' },
            postgresSyncEnabled: { type: 'boolean' },
            postgresSyncLoanDocumentsEnabled: { type: 'boolean' },
            postgresHost: { type: 'string' },
            postgresPort: { type: 'integer' },
            postgresDatabase: { type: 'string' },
            postgresUsername: { type: 'string', sensitive: true },
            postgresPassword: { type: 'string', sensitive: true },
            csPortalS3Endpoint: { type: 'string' },
            csPortalS3Bucket: { type: 'string' },
            csPortalS3Key: { type: 'string' },
            csPortalS3Secret: { type: 'string' },
            bsiBoardLoansEnabled: { type: 'boolean' },
            bsiRetrieveServiceRecordsEnabled: { type: 'boolean' },
            bsiRetrieveInternalRepurchaseQuotesEnabled: { type: 'boolean' },
            bsiHost: { type: 'string' },
            bsiPort: { type: 'integer' },
            bsiUsername: { type: 'string', sensitive: true },
            bsiPassword: { type: 'string', sensitive: true },
            blueWaterPullLoansEnabled: { type: 'boolean' },
            blueWaterUploadClosedLoanPackageDocumentsEnabled: { type: 'boolean' },
            blueWaterUrlBase: { type: 'string' },
            blueWaterUsername: { type: 'string', sensitive: true },
            blueWaterPassword: { type: 'string', sensitive: true },
            blueWaterSftpHost: { type: 'string' },
            blueWaterSftpPort: { type: 'integer' },
            blueWaterSftpUsername: { type: 'string', sensitive: true },
            blueWaterSftpPassword: { type: 'string', sensitive: true },
            blueWaterSftpDirName: { type: 'string' },
            pollyUsername: { type: 'string', sensitive: true },
            pollyPassword: { type: 'string', sensitive: true },
            pollyAuthToken: { type: 'string', sensitive: true },
            essexSftpHost: { type: 'string' },
            essexSftpPort: { type: 'integer' },
            essexSftpUsername: { type: 'string', sensitive: true },
            essexSftpPassword: { type: 'string', sensitive: true },
            essexSftpEnableDownload: { type: 'boolean' },
            bulkLoanDownloadSftpHost: { type: 'string' },
            bulkLoanDownloadSftpPort: { type: 'integer' },
            bulkLoanDownloadSftpUsername: { type: 'string', sensitive: true },
            bulkLoanDownloadSftpPassword: { type: 'string', sensitive: true },
            googleMapsApiKey: { type: 'string' },
            googleMapsUiApiKey: { type: 'string' },
            geocodeSyncEnabled: { type: 'boolean' },
            policyhubSyncEnabled: { type: 'boolean' },
            policyhubSftpHost: { type: 'string' },
            policyhubSftpPort: { type: 'number' },
            policyhubSftpUsername: { type: 'string', sensitive: true },
            policyhubSftpPassword: { type: 'string', sensitive: true },
            mctSyncEnabled: { type: 'boolean' },
            mctSyncUrlBase: { type: 'string' },
            mctSyncClientId: { type: 'string' },
            mctSyncClientSecret: { type: 'string' },
            mctSyncClientCode: { type: 'string' },
            solifyUrlBase: { type: 'string' },
            solifyApiToken: { type: 'string' },
            ccdAutomationWebhookUrl: { type: 'string' },
            ccdAutomationApiKey: { type: 'string' },
            insuranceApiUrl: { type: 'string' },
            insuranceApiToken: { type: 'string', sensitive: true },
            insuranceApiOauthId: { type: 'string' },
        }
    },

    AhLoanNumberSequence: {
        fields: {
            _id: { type: 'id' },
            lastAhLoanNumber: { type: 'integer' },
        }
    },

    ReconciliationReport: {
        fields: {
            _id: { type: 'id' },
            uploadedAt: { type: 'datetime' },
            correspondent: { type: 'Correspondent' },
            uploadedByName: { type: 'string' },
            uploadedByEmail: { type: 'string', validate: EMAIL_REGEXP },
            s3Key: { type: 'string' },
            fileName: { type: 'string' },
            contentType: { type: 'string' },
            processedAt: { type: 'datetime' },
            processedBy: { type: 'User' },
        }
    },

    ReconciliationBatch: {
        fields: {
            _id: { type: 'id' },
            uploadedAt: { type: 'datetime' },
            uploadedBy: { type: 'User' },
            mappedAt: { type: 'datetime' },
            mappedBy: { type: 'User' },
            correspondent: { type: 'Correspondent' },
            s3Key: { type: 'string' },
            fileName: { type: 'string' },
            contentType: { type: 'string' },
            status: { type: 'string', options: RECONCILIATION_STATUS_OPTIONS },
            processingCollateralAt: { type: 'datetime' },
            correspondentFeeWireReceivedAt: { type: 'datetime' },
            correspondentFeeWireReference: { type: 'string' },
            correspondentFeeWireAmount: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            trfaDpaReimbursementWireSentAt: { type: 'datetime' },
            trfaDpaReimbursementWireReference: { type: 'string' },
            trfaDpaReimbursementWireAmount: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            shippedToBotsOn: { type: 'dateonly', label: 'Shipped To BOTS On' },
            shippedToBotsTrackingNumber: { type: 'string' },
            finishedPreparingWlsImportAt: { type: 'datetime', label: 'Finished Preparing WLS Import At' },
            finishedSubmittingWlsImportAt: { type: 'datetime', label: 'Finished Submitting WLS Import At' },
            botsProcessingCompletedAt: { type: 'datetime', label: 'BOTS Processing Completed At' },

            totalDpaAmount: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 0 },
            totalLoanCount: { type: 'integer' },
        }
    },

    LoanMismo: {
        fields: {
            _id: { type: 'id' },
            uploadedAt: { type: 'datetime' },
            correspondent: { type: 'Correspondent' },
            s3Key: { type: 'string' },
        },
    },

    CensusFipsCode: {
        fields: {
            _id: { type: 'id' },
            state: { type: 'string' },
            summaryLevel: { type: 'string' },
            stateFipsCode: { type: 'string' },
            countyFipsCode: { type: 'string' },
            countySubdivisionFipsCode: { type: 'string' },
            placeFipsCode: { type: 'string' },
            consolidatedCityFipsCode: { type: 'string' },
            name: { type: 'string' },
        },
        indexes: [
            { fields: ['stateFipsCode', 'summaryLevel'] },
            { fields: ['state', 'summaryLevel'] },
        ]
    },

    EepInitialInfoDocForm: {
        fields: {
            _id: { type: 'id' },
            loan: { type: 'Loan' },
            disclosureDate: { type: 'dateonly' },
            programParticipationPercentage: { type: 'decimal', format: 'percent' },
            propertyAddress: { type: 'string' },
            propertyCity: { type: 'string' },
            propertyState: { type: 'string', options: US_STATE_CODES },
            propertyZip: { type: 'string' },
            purchasePrice: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 0  },
            appraisedValue: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 0  },
            fhaLoanInterestRate: { type: 'decimal', format: 'percent' },
            downPayment: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 0  },
            overrideDownPayment: { type: 'boolean' },
            upfrontMortgageInsurancePremium: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            overrideUpfrontMortgageInsurancePremium: { type: 'boolean' },
            fhaInsuredLoanAmount: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 0 },
            overrideFhaInsuredLoanAmount: { type: 'boolean' },
            principleAndInterest: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            overridePrincipleAndInterest: { type: 'boolean' },
            mortgageInsurance: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            overrideMortgageInsurance: { type: 'boolean' },
            annualPropertyTax: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            annualHazardInsurance: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            annualFloodInsurance: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            estimatedClosingCost: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            estimatedCashToClose: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            sellerConcessions: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            earnestMoneyDeposit: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            homebuyersVerifiedAssets: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 0 },
            monthlyHoaFee: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            mobileNotaryFee: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            hoaServiceFee: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            overrideHoaServiceFee: { type: 'boolean' },
            notaryFee: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            programUnderwritingFee: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
        }
    },

    EepFinalInfoDocForm: {
        fields: {
            _id: { type: 'id' },
            loan: { type: 'Loan' },
            disclosureDate: { type: 'dateonly' },
            programParticipationPercentage: { type: 'decimal', format: 'percent' },
            propertyAddress: { type: 'string' },
            propertyCity: { type: 'string' },
            propertyState: { type: 'string', options: US_STATE_CODES },
            propertyZip: { type: 'string' },
            purchasePrice: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 0  },
            appraisedValue: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 0  },
            fhaLoanInterestRate: { type: 'decimal', format: 'percent' },
            downPayment: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 0  },
            overrideDownPayment: { type: 'boolean' },
            upfrontMortgageInsurancePremium: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            overrideUpfrontMortgageInsurancePremium: { type: 'boolean' },
            fhaInsuredLoanAmount: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 0 },
            overrideFhaInsuredLoanAmount: { type: 'boolean' },
            principleAndInterest: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            overridePrincipleAndInterest: { type: 'boolean' },
            mortgageInsurance: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            overrideMortgageInsurance: { type: 'boolean' },
            annualPropertyTax: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            annualHazardInsurance: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            annualFloodInsurance: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            monthlyHoaFee: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            monthlyMortgagePayment: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            monthlyLeasePayment: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            closingCost: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            cashToClose: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            sellerConcessions: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            earnestMoneyDeposit: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            prepaidClosingCosts: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            otherCredits: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            homebuyersVerifiedAssets: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 0 },
            hoaServiceFee: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            overrideHoaServiceFee: { type: 'boolean' },
            invoiceHoaServiceFeeSeparately: { type: 'boolean' },
            notaryFee: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            invoiceNotaryFeeSeparately: { type: 'boolean' },
            mobileNotaryFee: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            invoiceMobileNotaryFeeSeparately: { type: 'boolean' },
            programUnderwritingFee: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            invoiceProgramUnderwritingFeeSeparately: { type: 'boolean' },
            provideHazardInsurance: { type: 'boolean' },
        }
    },

    EepHomeownershipAgreementDocForm: {
        fields: {
            _id: { type: 'id' },
            loan: { type: 'Loan' },
            propertyAddress: { type: 'string' },
            propertyCity: { type: 'string' },
            propertyState: { type: 'string', options: US_STATE_CODES },
            propertyZip: { type: 'string' },
            propertyCounty: { type: 'string' },
            legalPropertyDescription: { type: 'string' },
            taxParcelId: { type: 'string' },
            closingDate: { type: 'dateonly' },
            expirationDate: { type: 'dateonly' },
            purchasePrice: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 0  },
            appraisedValue: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 0  },
            firstPaymentDate: { type: 'dateonly' },
            firstMortgageInterestRate: { type: 'decimal', format: 'percent' },
            firstMortgageAmount: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 0 },
            overrideFirstMortgageAmount: { type: 'boolean' },
            secondMortgageAmount: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 0 },
        }
    },

    RateSheet: {
        fields: {
            _id: { type: 'id' },
            correspondent: { type: 'Correspondent' },
            createdAt: { type: 'datetime' },
            createdBy: { type: 'User' },
            effectiveDate: { type: 'dateonly' },
            rates: { type: 'RateSheetRate[]' },
            llpaManufacturedHome: { type: 'decimal', format: 'percent' },
            llpaTwoUnits: { type: 'decimal', format: 'percent' },
            llpaManualUnderwrite: { type: 'decimal', format: 'percent' },
            llpaIncomeExceeds160Ami: { type: 'decimal', format: 'percent' },
            llpaHighBalance: { type: 'decimal', format: 'percent' },
            llpaFico580: { type: 'decimal', format: 'percent' },
            llpaFico600: { type: 'decimal', format: 'percent' },
            llpaFico620: { type: 'decimal', format: 'percent' },
            llpaFico640: { type: 'decimal', format: 'percent' },
            llpaFico660: { type: 'decimal', format: 'percent' },
            llpaFico680: { type: 'decimal', format: 'percent' },
            llpaDpa35Lt100: { type: 'decimal', format: 'percent' },
            llpaDpa35100: { type: 'decimal', format: 'percent' },
            llpaDpa35120: { type: 'decimal', format: 'percent' },
            llpaDpa5Lt70: { type: 'decimal', format: 'percent' },
            llpaDpa570: { type: 'decimal', format: 'percent' },
            llpaDpa580: { type: 'decimal', format: 'percent' },
            llpaDpa590: { type: 'decimal', format: 'percent' },
        },
        indexes: [
            { fields: ['effectiveDate'] },
        ]
    },

    RateSheetRate: {
        nested: true,
        fields: {
            _key: { type: 'string' },
            _position: { type: 'number' },
            productType: { type: 'string', options: ['EEP', 'DPA'] },
            dpaRepaymentType: { type: 'string', options: ['Repayable', 'Forgivable'] },
            dpaPercent: { type: 'decimal', format: 'percent' },
            interestRate: { type: 'decimal', format: 'percent' },
            lockPeriod: { type: 'string', options: ['15', '30', '45', '60'] },
            price: { type: 'decimal', format: 'percent' },
        }
    },

    RateSheetRateAdjustment: {
        fields: {
            _id: { type: 'id' },
            correspondent: { type: 'Correspondent' },
            effectiveDate: { type: 'dateonly' },
            productType: { type: 'string', options: ['EEP', 'DPA'] },
            dpaRepaymentType: { type: 'string', options: ['Repayable', 'Forgivable'] },
            dpaPercent: { type: 'decimal', format: 'percent' },
            adjustment: { type: 'decimal' },
        }
    },

    RateSheetAdjustmentOverride: {
        fields: {
            _id: { type: 'id' },
            correspondent: { type: 'Correspondent' },
            effectiveDate: { type: 'dateonly' },
            name: { type: 'string' },
            override: { type: 'decimal' },
        }
    },

    GeneralDocument: {
        fields: {
            _id: { type: 'id' },
            name: { type: 'string' },
            s3Key: { type: 'string', label: 'S3 Key' },
            contentType: { type: 'string' },
            createdAt: { type: 'datetime' },
            documentType: {
                type: 'string',
                options: [
                    'BSI/Internal Repurchase Quote',
                    'Essex/Arrive Home FHA report',
                    'Essex/Bulk Data Arrive Home',
                    'Essex/Tule Insurance Report',
                ]
            }
        }
    },

    Holiday: {
        fields: {
            _id: { type: 'id' },
            date: { type: 'dateonly' },
            name: { type: 'string' },
            officeClosed: { type: 'boolean' },
            lockDeskClosesAt: { type: 'timeonly' },
        },
        indexes: [
            { fields: ['date'], unique: true },
        ]
    },

    AssignmentPool: {
        fields: {
            _id: { type: 'id' },
            assignmentType: { type: 'string', options: ['Purchase Clearing', 'Purchase Clearing Assistant', 'Funding Pre-Check', 'Funding Wire', 'MERS Transfer', 'Lock Desk', 'EEP Support Specialist', 'EEP Processor', 'EEP Underwriter', 'EEP Closer', 'Servicing Member'], required: true },
            assignmentPoolEntries: { type: 'AssignmentPoolEntry[]' },
            lastAssignedEntrySequence: { type: 'integer' },
            lastAssignedEntryKey: { type: 'string' },
            lastAssignedEntryCount: { type: 'integer' },
        },
        indexes: [
            { fields: ['assignmentType'], unique: true },
        ]
    },

    AssignmentPoolEntry: {
        nested: true,
        fields: {
            _key: { type: 'string' },
            _position: { type: 'number' },
            user: { type: 'User' },
            weight: { type: 'integer', gte: 1, required: true, },
            paused: { type: 'boolean' },
        }
    },

    NotificationList: {
        fields: {
            _id: { type: 'id' },
            notificationType: { type: 'string', options: ['Lock Requested', 'Condition Document Submitted', 'Reconciliation Batch Submitted', 'Ticket Submitted', 'EEP Processing Status Changed (Underwriting)'] },
            notificationListRecipients: { type: 'NotificationListRecipient[]' },
        },
        indexes: [
            { fields: ['notificationType'], unique: true },
        ]
    },

    NotificationListRecipient: {
        nested: true,
        fields: {
            _key: { type: 'string' },
            _position: { type: 'number' },
            user: { type: 'User' },
        }
    },

    CachedDashboard: {
        fields: {
            _id: { type: 'id' },
            generatedAt: { type: 'datetime' },
            screenResponseJson: { type: 'string' },
        },
    },

    CachedCorrespondentDashboard: {
        fields: {
            _id: { type: 'id' },
            correspondent: { type: 'Correspondent' },
            generatedAt: { type: 'datetime' },
            screenResponseJson: { type: 'string' },
        },
        indexes: [
            { fields: ['correspondentId'], unique: true },
        ]
    },

    Trade: {
        fields: {
            _id: { type: 'id' },
            commitmentNumber: { type: 'string' },
            status: { type: 'string', options: [
                'Committed',
                'Due Diligence Completed',
                'Certified',
                'Settled',
            ] },
            dealer: { type: 'Dealer' },
            einNumber: { type: 'string' },
            investor: { type: 'Investor' },
            commitmentType: { type: 'string', options: [
                'Mandatory Sell',
                'Best Efforts Sell',
                'MBS',
            ] },
            securityType: { type: 'string', options: [
                'GNMA I', // We probably don't need this option
                'GNMA II',
                'FNMA',
                'FHLMC',
                'UMBS',
            ] },
            programType: { type: 'string', options: [ 'Fixed', 'ARM' ] }, // Always Fixed right now
            coupon: { type: 'decimal', format: 'percent' },

            achRoutingTaxesAndInsurance: { type: 'string' },
            achAccountTaxesAndInsurance: { type: 'string' },
            achRoutingPrincipalAndInterest: { type: 'string' },
            achAccountPrincipalAndInterest: { type: 'string' },

            gnmaPoolIdentifier: { type: 'string', label: 'GNMA Pool Identifier' },
            gnmaPoolType: { type: 'string', label: 'GNMA Pool Type', options: [ 'SF', 'MH', 'JM', 'BD', 'ET', 'RG' ]},
            gnmaPoolCertificateInitialPaymentDate : { type: 'dateonly', label: 'GNMA Pool Certificate Initial Payment Date' },
            securityTradeBookEntryDate: { type: 'dateonly' },
            securityInvestorRoutingNumber: { type: 'string' },
            securityInvestorSubscriptionAmount: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 0 },
            issueDate: { type: 'dateonly' },
            maturityDate: { type: 'dateonly' },
            structureType: { type: 'string', options: [ 'Single Issuer', 'Multiple Issuer' ] },

            securityPrice: { type: 'decimal' },
            specPayup: { type: 'decimal' },
            totalPrice: { type: 'decimal' },
            tradeAmount: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            assignedAmount: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            tolerancePercent: { type: 'decimal', format: 'percent' },
            // average: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },

            commitmentDate: { type: 'dateonly' },
            dueDilligenceCompletedDate: { type: 'dateonly' },
            certifiedDate: { type: 'dateonly' },
            settledDate: { type: 'dateonly' },
            notificationDate: { type: 'dateonly' },
            servicingReleased: { type: 'boolean' },
            uploadedToGinnieNet: { type: 'boolean' },
        },
        indexes: [
            { fields: ['commitmentNumber'], unique: true },
        ],
    },

    LoanSale: {
        fields: {
            _id: { type: 'id' },
            saleNumber: { type: 'string' },
            accruedInterestDate: { type: 'dateonly' },
            purchasePricePercent: { type: 'decimal', format: 'percent' },
            investor: { type: 'Investor' },
            loanSaleAmount: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            status: { type: 'string', label: 'Loan Sale Status', options: [ 'New', 'Ready to Deliver', 'Delivered' ]},            
        },
    },    

    Dealer: {
        fields: {
            _id: { type: 'id' },
            name: { type: 'string' },
        }
    },

    BulkDownload: {
        fields: {
            _id: { type: 'id' },
            createdAt: { type: 'datetime' },
            createdBy: { type: 'User' },
            status: { type: 'string', options: ['Pending', 'Processing', 'Paused', 'Completed', 'Failed'] },
            progress: { type: 'integer', format: 'percentage' },
            sftpDirectory: { type: 'string' },
            hostName: { type: 'string' },
            loans: { type: 'BulkDownloadLoan[]' },
            documentTypes: { type: 'string[]' },
        }
    },

    BulkDownloadLoan: {
        nested: true,
        fields: {
            _key: { type: 'string' },
            _position: { type: 'number' },
            loan: { type: 'Loan' },
            startedAt: { type: 'datetime' },
            finishedAt: { type: 'datetime' },
        }
    },

    PolicyhubGroup: {
        fields: {
            _id: { type: 'id' },
            name: { type: 'string' },
            guid: { type: 'string' },
        },
    },

    WarehouseBatch: {
        fields: {
            _id: { type: 'id' },
            createdAt: { type: 'datetime' },
            submittedAt: { type: 'datetime' },
            status: { type: 'string', options: ['Preparing', 'Ready for Submission', 'Submitted', 'Purchased'] },
            warehouseBank: { type: 'WarehouseBank' },
            requestDate: { type: 'dateonly' },
            closingAgentOrderNumber: { type: 'string' },
            totalWireAmount: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            totalHaircutAmount: { type: 'decimal', format: 'dollars', commas: true, decimalPlaces: 2 },
            purchasedAt: { type: 'datetime' },
        }
    },

    PostgresLoanDocumentSyncState: {
        fields: {
            _id: { type: 'id' },
            lastSyncedPostgresId: { type: 'integer' },
        }
    },

    Announcement: {
        fields: {
            _id: { type: 'id' },
            title: { type: 'string' },
            message: { type: 'string' },
            createdAt: { type: 'datetime' },
            createdBy: { type: 'User' },
            dismissable: { type: 'boolean' },
            startShowingAt: { type: 'datetime' },
            stopShowingAt: { type: 'datetime' },
            visibleToUserTypes: { type: 'string[]', options: ['Internal', 'Correspondent'] },
            backgroundColor: { type: 'string', options: ['blue', 'red', 'orange', 'yellow', 'green'] }
        }
    },

    DismissedAnnouncement: {
        fields: {
            _id: { type: 'id' },
            announcementId: { type: 'id' },
            userId: { type: 'id' },
            dismissedAt: { type: 'datetime' },
        },
        indexes: [
            { fields: ['announcementId', 'userId'], unique: true },
        ]
    },

    BulkLoanUpdateBatch: {
        fields: {
            _id: { type: 'id' },
            createdAt: { type: 'datetime' },
            createdIp: { type: 'string' },
            createdByUser: { type: 'User' },
            createdByAdminUser: { type: 'User' },
            fileName: { type: 'string' },
            totalLoanCount: { type: 'integer' },
            processedCount: { type: 'integer' },
            readyAt: { type: 'datetime' },
            startedAt: { type: 'datetime' },
            completedAt: { type: 'datetime' },
        },
        indexes: [
            { fields: ['createdAt'] },
            { fields: ['completedAt'], partial: { completedAt: { $type: 'null' } }, },
        ]
    },

    BulkLoanUpdate: {
        fields: {
            _id: { type: 'id' },
            bulkLoanUpdateBatch: { type: 'BulkLoanUpdateBatch' },
            loanSaveParams: { type: 'any' },
        },
        indexes: [
            { fields: ['bulkLoanUpdateBatchId'] },
        ]
    },

    TicketNumberSequence: {
        fields: {
            _id: { type: 'id' },
            ticketNumber: { type: 'integer' },
        }
    },

    TicketRequest: {
        fields: {
            _id: { type: 'id' },
            ticketNumber: { type: 'integer' },
            author: { type: 'User' },
            createdAt: { type: 'datetime' },
            archivedAt: { type: 'datetime' },
            githubIssueUrl: { type: 'string' },

            ticketType: { type: 'string', options: [
                'Bug Fix',
                'Enhancement',
            ]},
            targetAudience: { type: 'string', options: [
                'Arrive Home Employees',
                'Correspondents',
            ]},
            screen: { type: 'string' },
            loanTypes: { type: 'string[]', options: [
                'DPA',
                'EEP',
                'White Label',
            ]},
            description: { type: 'string', lines: 4 },
            priority: { type: 'string', options: [
                'Low',
                'Moderate',
                'High',
                'Critical',
            ]},
        },
        indexes: [
            { fields: ['ticketNumber'], unique: true },
            { fields: ['authorId'] },
        ],
    },

    TicketRequestDocument: {
        fields: {
            _id: { type: 'id' },
            name: { type: 'string' },
            s3Key: { type: 'string', label: 'S3 Key' },
            contentType: { type: 'string' },
            uploadedAt: { type: 'datetime' },
            ticketRequest: { type: 'TicketRequest' },
        },
        indexes: [ { fields: ['ticketRequestId'] }, ]
    },
    
    CcdAutomationJob: {
        fields: {
            _id: { type: 'id' },
            inputLoanDocument: { type: 'LoanDocument' },
            outputLoanDocument: { type: 'LoanDocument' },
            status: { type: 'string', options: ['Pending', 'Complete'] },
        },
    },

    LoanOfficer: {
        fields: {
            _id: { type: 'id' },
            createdAt: { type: 'datetime' },
            name: { type: 'string', required: true },
            email: { type: 'string', validate: EMAIL_REGEXP, required: true },
            phoneNumber: { type: 'string', validate: PHONE_NUMBER_REGEXP, required: true },
            nmlsNumber: { type: 'string', required: true },
            rating: { type: 'string', options: ['Green', 'Yellow', 'Red'] }
        },
        indexes: [ { fields: ['nmlsNumber'], unique: true }, ]
    },

    IncomeDetail: {
        nested: true,
        fields: {
            _key: { type: 'string' },
            borrowerPosition: { type: 'integer' },
            employerName: { type: 'string' },
            selfEmployed: { type: 'boolean' },
            incomeType: { type: 'string' },
            monthlyAmount: { type: 'decimal', format: 'dollars', commas: true },
        }
    },

    Income: {
        nested: true,
        fields: {
            _key: { type: 'string' },
            incomeType: { type: 'string', options: ['Salaried', 'Self-employed', 'Retired', 'Disabled', 'Other'] },
            base: { type: 'decimal', format: 'dollars', commas: true },
            overtime: { type: 'decimal', format: 'dollars', commas: true },
            bonus: { type: 'decimal', format: 'dollars', commas: true },
            commission: { type: 'decimal', format: 'dollars', commas: true },
            dividendsAndInterest: { type: 'decimal', format: 'dollars', commas: true },
            netRentalIncome: { type: 'decimal', format: 'dollars', commas: true },
            otherIncome: { type: 'decimal', format: 'dollars', commas: true },
            borrowerPosition: { type: 'integer' },
        }
    },

    Liability: {
        nested: true,
        fields: {
            _key: { type: 'string' },
            creditorName: { type: 'string' },
            monthlyPayment: { type: 'decimal', format: 'dollars', commas: true },
            balance: { type: 'decimal', format: 'dollars', commas: true },
            borrowerPosition: { type: 'integer' },
        }
    },

    Asset: {
        nested: true,
        fields: {
            _key: { type: 'string' },
            institutionName: { type: 'string' },
            accountNumber: { type: 'string', sensitive: true },
            statementDate: { type: 'dateonly' }, // TODO: where do I get this from the mismo? Keep looking in spec.
            balance: { type: 'decimal', format: 'dollars', commas: true, sensitive: true },
            borrowerPosition: { type: 'integer' },
        }
    },

    DelayedEmailNotification: {
        fields: {
            _id: { type: 'id' },
            sendAt: { type: 'datetime' },
            notificationType: {
                type: 'string',
                options: [
                    'Loan Condition Document Submitted (Prior To Close)',
                    'Loan Condition Document Submitted (Closing Request)',
                    'Loan Condition Document Submitted (Purchase Clearing)',
                    'Loan Condition Document Submitted (Post Purchase)',
                ]
            },
            loan: { type: 'Loan' },
        },
    },
};

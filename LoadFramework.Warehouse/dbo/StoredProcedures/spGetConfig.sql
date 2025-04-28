CREATE PROCEDURE dbo.spGetConfig(
	@ConfigName VARCHAR(200)
)
AS
BEGIN
	/*
		created by:			Raphael Sayegh

		Sample Call
		EXEC dbo.spGetConfig
			--@ConfigName = 'Initialization - Audit'
			--@ConfigName = 'Capacity Metrics - Alerts'
			--@ConfigName ='Capacity Metrics - Classification'
			@ConfigName ='Initialization'

	*/
    SELECT 
        JSON_OBJECT(
            'pConfigId' : [ConfigId],
            'pConfigName' : [ConfigName],
            'pAuditLogTimeframeInMinutes' : [AuditLogTimeframeInMinutes],
            'pAllActivities' : [AllActivities],
            'pInitialization_Audit' : [Initialization_Audit],
            'pAuditLog' : [ExportAudit],
			'pLastProcessedDateAndTime_Audit' : [LastProcessedDateAndTime_Audit],
            'pkeyVaultName' : [keyVaultName],
            'pTenantId' : [TenantId],
            'pDomainName' : [DomainName],
            'pFabricSpnClientId' : [FabricSpnClientId],
            'pFabricSpnSecretName' : [FabricSpnSecretName],
            'pFabricSpnAdminConsentClientId' : [FabricSpnAdminConsentClientId],
            'pFabricSpnAdminConsentSecretName' : [FabricSpnAdminConsentSecretName],
            'pAdmin' : [Admin],
            'pAdminSecretName' : [AdminSecretName],
            'pFabricSecurityGroupId' : [FabricSecurityGroupId],
            'pReloadDates' : [ReloadDates],
            'pStartDate' : [StartDate],
            'pEndDate' : [EndDate],
            'pInitialization_Inventory' : [Initialization_Inventory],
            'pThrottleScanApi' : [ThrottleScanApi],
            'pLastProcessedDateAndTime_Inventory' : [LastProcessedDateAndTime_Inventory],
            'pTenantMetadata' : [ExportTenantMetadata],
            'pGatewayClusters' : [ExportGatewayClusters],
            'pInventory' : [ExportInventory],
            'pDatasetRefreshHistory' : [ExportDatasetRefreshHistory],
            'pTopNRefreshHistory' : [TopNRefreshHistory]
        ) AS Configuration
    FROM 
        [dbo].[vwConfig]
    WHERE 
        [ConfigName] = @ConfigName
END
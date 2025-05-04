/*
	DELETE FROM [dbo].[Config]
	DELETE FROM [dbo].[ConfigAudit]
	DELETE FROM [dbo].[ConfigAzure]
	DELETE FROM [dbo].[ConfigCapacityMetrics]
	DELETE FROM [dbo].[ConfigDate]
	DELETE FROM [dbo].[ConfigInventory]

	DELETE FROM [dbo].[Load]
	DELETE FROM [dbo].[LoadLog]
	DELETE FROM [dbo].[LoadProcessedDate]
*/

INSERT INTO dbo.Config ([ConfigId],[ConfigName],[CreatedDate],[CreatedBy],[IsCurrent])
VALUES(1, 'Initialization', GETDATE(), 'rsayegh', 1)


INSERT INTO dbo.ConfigAudit ([ConfigId],[AuditLogTimeframeInMinutes],[AllActivities],[Initialization], [LastProcessedDateAndTime])
VALUES(1,'60','yes', 'yes', NULL)

INSERT INTO dbo.ConfigAzure ([ConfigId],[keyVaultName],[TenantId],[DomainName],[FabricSpnClientId],[FabricSpnSecret],[FabricSpnAdminConsentClientId],[FabricSpnAdminConsentSecret],[FabricSecurityGroupId]) 
VALUES(
    1
    ,'your-key-vault-name'
    ,'AzureTenantId'
    ,'DomainName'
    ,'FabricSpnId'
    ,'FabricSpnSecret'
    ,'FabricSpnAdminConsentId'
    ,'FabricSpnAdminConsentSecret'
	,'FabricSecurityGroupId'
)


INSERT INTO dbo.ConfigDate([ConfigId],[StartDate],[EndDate])
VALUES(1,'2025-01-01','2025-12-31')

INSERT INTO dbo.ConfigInventory (
     [ConfigId] 
    ,[Initialization] 
    ,[ThrottleScanApi] 
    ,[LastProcessedDateAndTime] 
    ,[ExportTenantMetadata] 
    ,[ExportGatewayClusters] 
    ,[ExportInventory] 
    ,[ExportDatasetRefreshHistory] 
    ,[TopNRefreshHistory]
)
VALUES(1,'yes','yes', NULL,'yes','yes','yes','yes', '0')

-- Auto Generated (Do not modify) 9FAC87B2152AC598AF0C7C226362189796A25C0801B626D3BF09E31FD332A5A6

CREATE VIEW dbo.vwConfig
AS 
	/*
		Sample call:

		SELECT * FROM dbo.vwConfig
	*/
	SELECT 
		 c.[ConfigId]
		,c.[ConfigName]
		--,ISNULL(ca.[AuditLogDays], 'no') AS [AuditLogDays]
		,ISNULL(ca.[AuditLogTimeframeInMinutes], 60) AS [AuditLogTimeframeInMinutes]
		,ISNULL(ca.[AllActivities], 'no') AS [AllActivities]
		,ISNULL(ca.[Initialization], 'no') AS [Initialization_Audit]
		,CASE WHEN ca.[ConfigId] IS NULL THEN 'no' ELSE 'yes' END AS [ExportAudit]
		,ISNULL(LEFT(CAST(ca.[LastProcessedDateAndTime] AS VARCHAR(200)),19),'1900-01-01 00:00:00') AS [LastProcessedDateAndTime_Audit]
		,caz.[keyVaultName]
		,caz.[TenantId]
		,caz.[DomainName]
		,caz.[FabricSpnClientId]
		,caz.[FabricSpnSecretName]
		,caz.[FabricSpnAdminConsentClientId]
		,caz.[FabricSpnAdminConsentSecretName]
		,caz.[Admin]
		,caz.[AdminSecretName]
		,caz.[FabricSecurityGroupId]
		,CASE WHEN cd.[ConfigId] IS NULL THEN 'no' ELSE 'yes' END AS [ReloadDates]
		,ISNULL(CAST(cd.[StartDate] AS VARCHAR(10)),'1900-01-01 00:00:00') AS [StartDate]
		,
		ISNULL(
			CASE 
				WHEN DATEDIFF(DAY, GETDATE(), cd.[EndDate]) < 15 THEN CAST(EOMONTH(DATEADD(DAY,1, cd.[EndDate])) AS VARCHAR(10)) 
				ELSE CAST(cd.[EndDate] AS VARCHAR(10)) 
			END
			,'1900-01-01 00:00:00'
		) AS [EndDate]    
		,ISNULL(ci.[Initialization], 'no') AS  [Initialization_Inventory]
		,ISNULL(ci.[ThrottleScanApi], 'no') AS [ThrottleScanApi]
		,ISNULL(LEFT(CAST(ci.[LastProcessedDateAndTime] AS VARCHAR(200)),19),'1900-01-01 00:00:00') AS [LastProcessedDateAndTime_Inventory]
		,ISNULL(ci.[ExportTenantMetadata], 'no') AS [ExportTenantMetadata]
		,ISNULL(ci.[ExportGatewayClusters], 'no') AS [ExportGatewayClusters]
		,ISNULL(ci.[ExportInventory], 'no') AS [ExportInventory]
		,ISNULL([ExportDatasetRefreshHistory], 'no') AS [ExportDatasetRefreshHistory]
		,ISNULL([TopNRefreshHistory], '0') AS [TopNRefreshHistory] 
	FROM  
		[dbo].[Config] c
		LEFT JOIN [dbo].[ConfigAudit] ca
			ON ca.[ConfigId] = c.[ConfigId]
		INNER JOIN [dbo].[ConfigAzure] caz
			ON caz.[ConfigId] = c.[ConfigId]
		LEFT JOIN [dbo].[ConfigDate] cd
			ON cd.[ConfigId] = c.[ConfigId]
		LEFT JOIN [dbo].[ConfigInventory] ci
			ON ci.[ConfigId] = c.[ConfigId]        
	WHERE 
		c.[IsCurrent] = 1
;
CREATE TABLE [dbo].[ConfigInventory] (

	[ConfigId] bigint NOT NULL, 
	[Initialization] varchar(3) NULL, 
	[ThrottleScanApi] varchar(3) NULL, 
	[LastProcessedDateAndTime] datetime2(0) NULL, 
	[ExportTenantMetadata] varchar(3) NULL, 
	[ExportGatewayClusters] varchar(3) NULL, 
	[ExportInventory] varchar(3) NULL, 
	[ExportDatasetRefreshHistory] varchar(3) NULL, 
	[TopNRefreshHistory] varchar(10) NULL
);
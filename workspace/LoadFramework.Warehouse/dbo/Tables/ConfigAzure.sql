CREATE TABLE [dbo].[ConfigAzure] (

	[ConfigId] bigint NOT NULL, 
	[keyVaultName] varchar(200) NOT NULL, 
	[TenantId] varchar(200) NOT NULL, 
	[DomainName] varchar(200) NOT NULL, 
	[FabricSpnClientId] varchar(200) NOT NULL, 
	[FabricSpnSecret] varchar(200) NOT NULL, 
	[FabricSpnAdminConsentClientId] varchar(200) NOT NULL, 
	[FabricSpnAdminConsentSecret] varchar(200) NOT NULL, 
	[FabricSecurityGroupId] varchar(200) NOT NULL
);
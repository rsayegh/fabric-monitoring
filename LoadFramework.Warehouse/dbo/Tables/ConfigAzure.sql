CREATE TABLE [dbo].[ConfigAzure] (

	[ConfigId] bigint NOT NULL, 
	[keyVaultName] varchar(200) NOT NULL, 
	[TenantId] varchar(200) NOT NULL, 
	[DomainName] varchar(200) NOT NULL, 
	[FabricSpnClientId] varchar(200) NOT NULL, 
	[FabricSpnSecretName] varchar(200) NOT NULL, 
	[FabricSpnAdminConsentClientId] varchar(200) NOT NULL, 
	[FabricSpnAdminConsentSecretName] varchar(200) NOT NULL, 
	[Admin] varchar(200) NOT NULL, 
	[AdminSecretName] varchar(200) NOT NULL, 
	[FabricSecurityGroupId] varchar(200) NOT NULL
);
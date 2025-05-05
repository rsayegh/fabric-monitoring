CREATE TABLE [dbo].[ConfigAudit] (

	[ConfigId] bigint NOT NULL, 
	[AuditLogTimeframeInMinutes] varchar(200) NOT NULL, 
	[AllActivities] varchar(200) NOT NULL, 
	[Initialization] varchar(200) NOT NULL, 
	[LastProcessedDateAndTime] datetime2(0) NULL
);
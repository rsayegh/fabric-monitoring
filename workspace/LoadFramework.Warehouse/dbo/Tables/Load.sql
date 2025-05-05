CREATE TABLE [dbo].[Load] (

	[LoadId] bigint NOT NULL, 
	[ModuleRunId] varchar(200) NOT NULL, 
	[PipelineTriggerDt] datetime2(0) NULL, 
	[PipelineStatus] varchar(200) NULL, 
	[TriggerId] varchar(200) NULL, 
	[Module] varchar(200) NULL, 
	[StartDate] datetime2(0) NULL, 
	[EndDate] datetime2(0) NULL, 
	[EntRunId] varchar(200) NULL, 
	[LastUpdate] datetime2(0) NOT NULL, 
	[CreatedBy] varchar(200) NOT NULL
);
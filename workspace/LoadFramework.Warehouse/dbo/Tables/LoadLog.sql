CREATE TABLE [dbo].[LoadLog] (

	[LoadLogId] bigint NOT NULL, 
	[LoadId] bigint NOT NULL, 
	[PipelineName] varchar(200) NOT NULL, 
	[InvokedPipeline] varchar(200) NULL, 
	[InvokedNotebook] varchar(200) NULL, 
	[LoadStatus] varchar(200) NOT NULL, 
	[LastUpdate] datetime2(0) NOT NULL, 
	[CreatedBy] varchar(200) NOT NULL
);
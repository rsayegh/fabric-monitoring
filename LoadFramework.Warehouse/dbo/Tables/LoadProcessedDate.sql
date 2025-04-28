CREATE TABLE [dbo].[LoadProcessedDate] (

	[LoadProcessedDateId] bigint NOT NULL, 
	[LoadId] bigint NOT NULL, 
	[PipelineName] varchar(200) NOT NULL, 
	[InvokedPipeline] varchar(200) NULL, 
	[InvokedNotebook] varchar(200) NULL, 
	[ProcessingType] varchar(200) NOT NULL, 
	[ProcessedDateAndTime] datetime2(0) NULL, 
	[ProcessingStatus] varchar(200) NULL, 
	[LastUpdate] datetime2(0) NOT NULL, 
	[CreatedBy] varchar(200) NOT NULL
);
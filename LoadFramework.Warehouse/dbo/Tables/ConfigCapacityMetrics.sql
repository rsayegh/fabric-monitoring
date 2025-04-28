CREATE TABLE [dbo].[ConfigCapacityMetrics] (

	[ConfigId] bigint NOT NULL, 
	[WorkspaceName] varchar(200) NOT NULL, 
	[DatasetName] varchar(200) NOT NULL, 
	[LastProcessedDateAndTime] datetime2(0) NULL, 
	[ExportCapacityMetrics] varchar(200) NOT NULL, 
	[ExportCapacityMetricsType] varchar(200) NOT NULL, 
	[CapacityThreshold] varchar(200) NOT NULL, 
	[ClassificationThreshold] varchar(200) NOT NULL, 
	[KnapsackCalculation] varchar(200) NOT NULL
);
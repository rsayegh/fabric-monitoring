CREATE TABLE [dbo].[Config] (

	[ConfigId] bigint NOT NULL, 
	[ConfigName] varchar(200) NOT NULL, 
	[CreatedDate] datetime2(0) NOT NULL, 
	[CreatedBy] varchar(200) NOT NULL, 
	[IsCurrent] bit NOT NULL
);
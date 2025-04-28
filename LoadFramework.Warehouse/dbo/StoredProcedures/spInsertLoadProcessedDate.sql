CREATE PROCEDURE dbo.spInsertLoadProcessedDate(
	 @LoadId BIGINT
	,@PipelineName VARCHAR(200)
	,@InvokedPipeline VARCHAR(200) = NULL 
	,@InvokedNotebook VARCHAR(200) = NULL
	,@ProcessingType VARCHAR(200)
	,@ProcessedDateAndTime VARCHAR(200) 
	,@ProcessingStatus VARCHAR(200)
)  
AS
BEGIN
	/*
		created by:			Raphael Sayegh

		Sample Call

		EXEC dbo.spInsertLoadProcessedDate
			 @LoadId = 1
			,@PipelineName = 'pl_platform_monitoring_execution'
			,@InvokedPipeline = 'pl_platform_monitoring_audit'
			,@InvokedNotebook = NULL
			,@ProcessingType = 'audit-log'
			,@ProcessedDateAndTime = '2023-10-20 01:00:00'
			,@ProcessingStatus = 'Failed'

		DECLARE 
			 @LoadId BIGINT = 8
			,@PipelineName VARCHAR(200) = 'pl_platform_monitoring_execution'
			,@InvokedPipeline VARCHAR(200) = 'pl_platform_monitoring_audit'
			,@InvokedNotebook VARCHAR(200) = NULL
			,@ProcessingType VARCHAR(200) = 'audit-log'
			,@ProcessedDateAndTime VARCHAR(200) = '2023-10-20 01:00:00'
			,@ProcessingStatus VARCHAR(200) = 'Failed'

		--Statuses :  Failed/Succeeded
	*/


	/* Begin the transaction */
	BEGIN TRANSACTION ;

		DECLARE  @LoadProcessedDateId BIGINT
				,@SysUser VARCHAR(200) = SUSER_SNAME()
				,@ConfigId BIGINT 

        SET @LoadProcessedDateId = 
        (
            SELECT ISNULL(MAX(LoadProcessedDateId),0) +1 AS LoadId
            FROM [dbo].[LoadProcessedDate]
            -- WHERE [Module] = @Module
        )

		SET @ConfigId = 
		(
			SELECT MAX([ConfigId])
			FROM [dbo].[Config]
		)

		INSERT INTO [dbo].[LoadProcessedDate](
			 [LoadProcessedDateId] 
			,[LoadId] 
			,[PipelineName] 
			,[InvokedPipeline]
			,[InvokedNotebook] 
			,[ProcessingType] 
			,[ProcessedDateAndTime] 
			,[ProcessingStatus] 
			,[LastUpdate]
			,[CreatedBy] 
		)
		SELECT 		 
			 @LoadProcessedDateId
			,@LoadId 
			,@PipelineName 
			,@InvokedPipeline 
			,@InvokedNotebook 
			,@ProcessingType 
			,CAST(@ProcessedDateAndTime AS DATETIME2(0))
			--,@ProcessingStatus
			,CASE WHEN @ProcessedDateAndTime = '2023-10-18 05:00:00' THEN 'Failed' ELSE @ProcessingStatus END 
			,GETDATE()
			,@SysUser


	/* Commit transation */
	COMMIT TRANSACTION ;

END
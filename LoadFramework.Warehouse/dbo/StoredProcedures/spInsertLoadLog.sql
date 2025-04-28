CREATE PROCEDURE dbo.spInsertLoadLog(
	 @LoadId BIGINT
	,@PipelineName VARCHAR(200)
	,@InvokedPipeline VARCHAR(200) = NULL 
	,@InvokedNotebook VARCHAR(200) = NULL
	,@LoadStatus VARCHAR(200)
)  
AS
BEGIN
	/*
		created by:			Raphael Sayegh

		Sample Call

		EXEC dbo.spInsertLoadLog
			 @LoadId = 1
			,@PipelineName = 'pl_platform_monitoring_execution'
			,@InvokedPipeline = 'pl_platform_monitoring_audit'
			,@InvokedNotebook = NULL
			,@LoadStatus = 'Succeeded'

		DECLARE 
			 @LoadId BIGINT = 8
			,@PipelineName VARCHAR(200) = 'pl_platform_monitoring_execution'
			,@InvokedPipeline VARCHAR(200) = 'pl_platform_monitoring_audit'
			,@InvokedNotebook VARCHAR(200) = NULL
			,@LoadStatus VARCHAR(200) = 'Succeeded'

		--Statuses : Created > Started/Cancelled > Failed/Succeeded
	*/


	/* Begin the transaction */
	BEGIN TRANSACTION ;

		DECLARE @LoadILogd BIGINT
        DECLARE @SysUser VARCHAR(200) = SUSER_SNAME()

        SET @LoadILogd = 
        (
            SELECT ISNULL(MAX(LoadLogId),0) +1 AS LoadId
            FROM dbo.LoadLog
            -- WHERE [Module] = @Module
        )

		INSERT INTO [dbo].[LoadLog](
			 [LoadLogId] 
			,[LoadId] 
			,[PipelineName] 
			,[InvokedPipeline] 
			,[InvokedNotebook] 
			,[LoadStatus] 
			,[LastUpdate]
			,[CreatedBy]
		)
		SELECT 		 
			 @LoadILogd
			,@LoadId 
			,@PipelineName 
			,@InvokedPipeline 
			,@InvokedNotebook 
			,@LoadStatus 
			,GETDATE()
			,@SysUser

	/* Commit transation */
	COMMIT TRANSACTION ;

END
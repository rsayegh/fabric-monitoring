CREATE PROCEDURE dbo.spInsertLoad(
	 @ModuleRunId  VARCHAR(200)
	,@PipelineTriggerDT  DATETIME2(1)
	,@TriggerId VARCHAR(200)
	,@Module VARCHAR(200)
	,@StartDate DATETIME2(1)
	,@EntRunId VARCHAR(200)
)  
AS
BEGIN
	/*
		created by:			Raphael Sayegh

		Sample Call

		EXEC dbo.spInsertLoad
			 @ModuleRunId = ''
			,@PipelineTriggerDT = '' 
			,@TriggerId = ''
			,@Module = 'platform-monitoring'
			,@StartDate = ''
			,@EntRunId = ''

		DECLARE 
			 @ModuleRunId VARCHAR(200) = ''
			,@PipelineTriggerDT DATETIME2(0) = GETDATE()  
			,@TriggerId VARCHAR(200) = ''
			,@Module VARCHAR(200) = 'platform-monitoring'
			,@StartDate DATETIME2(0) = GETDATE()
			,@EntRunId VARCHAR(200) = ''


		--Statuses : Created > Started/Cancelled > Failed/Succeeded
	*/

	/* Begin the transaction */
	BEGIN TRANSACTION ;

		DECLARE @LoadId BIGINT
        DECLARE @SysUser VARCHAR(200) = SUSER_SNAME()

        SET @LoadId = 
        (
            SELECT ISNULL(MAX(LoadId),0) +1 AS LoadId
            FROM dbo.Load 
            -- WHERE [Module] = @Module
        )
        -- SELECT @LoadId

		/* insert the load */
		INSERT INTO dbo.Load (
             LoadId 
            ,ModuleRunId
            ,PipelineTriggerDt
            ,PipelineStatus
            ,TriggerId
            ,[Module]
            ,StartDate
            ,EndDate
            ,EntRunId
            ,LastUpdate
            ,CreatedBy
		)
		SELECT
			 @LoadId
            ,@ModuleRunId
			,@PipelineTriggerDT
			,'Created'
			,@TriggerId
			,@Module
			,NULL
			,NULL 
			,@EntRunId
			,GETDATE()
			,@SysUser
		;

	/* Commit transation */
	COMMIT TRANSACTION ;


END
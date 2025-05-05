CREATE PROCEDURE dbo.spUpdateLoad(
	 @LoadId BIGINT
	,@Module VARCHAR(100)
	,@PipelineStatus VARCHAR(200)
)  
AS
BEGIN
	/*
		created by:			Raphael Sayegh

		Sample Call
		EXEC ETL.spUpdateLoad
			 @LoadId = 5
			,@Module = 'platform-monitoring'
			,@PipelineStatus = 'Cancelled'

		DECLARE 
			 @LoadId BIGINT = 5
			,@Module VARCHAR(100) = 'platform-monitoring'
			,@PipelineStatus VARCHAR(200) = 'Cancelled'
	*/

	/* Begin the transaction */
	BEGIN TRANSACTION ;

		/* Update status columns from ExecutionLog table */
		UPDATE 
			dbo.Load
		SET 
			 PipelineStatus = @PipelineStatus
			,StartDate = CASE WHEN StartDate IS NULL THEN GETDATE() ELSE StartDate END
			,EndDate = GETDATE()
			,LastUpdate = GETDATE()
		WHERE 
			LoadId = @LoadID
			AND 
			Module = @Module

	/* Commit transation */
	COMMIT TRANSACTION ;

END
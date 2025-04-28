CREATE PROCEDURE dbo.spStartLoad(
	 @LoadId BIGINT
	,@Module VARCHAR(100)
)  
AS
BEGIN
	/*
		created by:			Raphael Sayegh

		Sample Call
		EXEC dbo.spStartLoad
			 @LoadId = 1
			,@Module = 'platform-monitoring'

		DECLARE 
			 @LoadId BIGINT = 1
			,@Module VARCHAR(200) = 'platform-monitoring'
	*/

	/* Begin the transaction */
	BEGIN TRANSACTION ;
	
		/* check whether load is startable (no other load running in parallel) */
		IF EXISTS (
			SELECT 1
			FROM 
				dbo.Load
			WHERE 
				-- Module = @Module
				-- AND 
				PipelineStatus = 'Started'
		)
		BEGIN
			RAISERROR('Load cannot be started as another load needs to finish first. Wait until the currently running load is finished and re-try!', 16, 3) ;
			ROLLBACK TRANSACTION ;
		END


		/* Mark load as started */
		UPDATE dbo.Load
		SET 
			[PipelineStatus] = 'Started'
			,StartDate = GETDATE()
		WHERE 
			LoadId = @LoadId ;

	/* commit the transaction */
	COMMIT ;


END
CREATE PROCEDURE dbo.spUpdateLastProcessedDateAndTime(
	 @LoadId BIGINT
	,@ConfigName VARCHAR(200)
	,@ExtractionType VARCHAR(200)
	,@ExtractionDateTime VARCHAR(200)
)  
AS
BEGIN
	/*
		created by:			Raphael Sayegh

		Sample Call

		EXEC dbo.spUpdateLastProcessedDateAndTime
			 @LoadId = 1
			,@ConfigName = 'Initialization'
			,@ExtractionType = 'audit-log'
			,@ExtractionDateTime = '2025-04-08 00:00:00'

		DECLARE 
			 @LoadId BIGINT = 8
			,@ConfigName VARCHAR(200) = 'Initialization'
			,@ExtractionType VARCHAR(200) = 'audit-log'
			,@ExtractionDateTime VARCHAR(200) = '2025-04-08 00:00:00'

	*/



	/* Begin the transaction */
	BEGIN TRANSACTION ;

		/* get the config id */
		DECLARE @ConfigId BIGINT
		SELECT @ConfigId = ConfigId FROM dbo.Config WHERE ConfigName = @ConfigName

		/* update the ConfigAudit table */
		IF @ExtractionType = 'audit-log' 
		BEGIN
			/* update the table ConfigAudit */
			UPDATE [dbo].[ConfigAudit]
			SET [LastProcessedDateAndTime] = @ExtractionDateTime 
			WHERE ConfigId = @ConfigId
		END

		IF @ExtractionType = 'inventory' 
		BEGIN
			/* update the ConfigInventory table */
			UPDATE [dbo].[ConfigInventory]
			SET [LastProcessedDateAndTime] = @ExtractionDateTime 
			WHERE ConfigId = @ConfigId
		END


	/* Commit transation */
	COMMIT TRANSACTION ;

END
CREATE PROCEDURE dbo.spGetConfigPremiumMetricsDates(
     @ConfigId VARCHAR(200)
	,@AuditInitialization VARCHAR(200)
	,@AuditLogDays VARCHAR(200)
)
AS
BEGIN
	/*
		created by:			Raphael Sayegh

		Sample Call
		EXEC dbo.spGetConfigPremiumMetricsDates
			 @ConfigId = '1'
			 ,@AuditInitialization = 'no'
			 ,@AuditLogDays = '7'

		DECLARE 
			  @ConfigId BIGINT = 1
			 ,@AuditInitialization VARCHAR(200) = 'no'
			 ,@AuditLogDays VARCHAR(200) = '7'
	*/

	/* set the start and end date and time */
	DECLARE @StartDate DATE 
	DECLARE @EndDate DATE 

	DECLARE @StartDateTime DATETIME2(0)
	DECLARE @EndDateTime DATETIME2(0)
	

	/* chech if the audit is initialized */
	IF @AuditInitialization = 'yes'
	BEGIN

		/* set the start and end date */
		SET @EndDate = DATEADD(DAY, -1, CAST(GETDATE() AS DATE))
		SET @StartDate = DATEADD(DAY,-CAST(@AuditLogDays AS INT), @EndDate)
		--SELECT @StartDate,@EndDate

		/* Initialization - select the hours for which the audit will be extracted */
		SELECT 
			LEFT(CAST([DateAndTime] AS VARCHAR(200)),19) AS [DateAndTime]
		FROM 
			[MonitoringLake].[dbo].[date]
		WHERE 
			[Date] BETWEEN @StartDate AND @EndDate
			AND 
			[IsWholeHour] = 1
		ORDER BY 
			1

	END 
	ELSE 
	/* if not get the delta hours */
	BEGIN
		
		/* set the startdatetime and enddatetime */
		SELECT @StartDateTime = [LastExtractedDateTime]
		FROM 
			[dbo].[ConfigPremiumMetrics]
		WHERE 
			[ConfigId] = @ConfigId

		SET @EndDateTime = DATEADD(SECOND, -1, CAST(CAST(GETDATE() AS DATE) AS DATETIME))
		--SELECT @StartDateTime, @EndDateTime

		SELECT 
			LEFT(CAST([DateAndTime] AS VARCHAR(200)),19) AS [DateAndTime]
		FROM 
			[MonitoringLake].[dbo].[date]
		WHERE 
			[DateAndTime] BETWEEN @StartDateTime AND @EndDateTime
			AND 
			[IsWholeHour] = 1
		ORDER BY 
			1
	END


END
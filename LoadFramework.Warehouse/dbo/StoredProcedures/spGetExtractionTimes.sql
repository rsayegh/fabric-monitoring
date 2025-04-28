CREATE PROCEDURE dbo.spGetExtractionTimes(
     @ConfigId VARCHAR(200)
	,@Initialization VARCHAR(200)
	,@ExtractionType VARCHAR(200)
	,@AuditLogDays VARCHAR(200)
)
AS
BEGIN
	/*
		created by:			Raphael Sayegh

		--Sample Call
		EXEC dbo.spGetExtractionTimes
			 @ConfigId = '1'
			 ,@Initialization = 'na'
			 ,@ExtractionType = 'audit-log'
			 ,@AuditLogDays = '3'

			 --@ConfigId = '1'
			 --,@Initialization = 'na'
			 --,@ExtractionType = 'capacity-metrics'
			 --,@AuditLogDays = 'na'


		DECLARE 
			  @ConfigId VARCHAR(200) = '1'
			 ,@Initialization VARCHAR(200) = 'yes'
			 ,@ExtractionType VARCHAR(200) = 'audit-log'
			 ,@AuditLogDays VARCHAR(200) = '3'
	*/


	/* capacity-metrics */
	/* for the capacity metrics - the extraction has to be at the hour level - we extract the hours of the day before */
	IF @ExtractionType = 'capacity-metrics'
	BEGIN
		
		DECLARE @Date DATE = CAST(DATEADD(DAY, -1, GETDATE()) AS DATE)

		SELECT
			CAST(LEFT(CAST([DateAndTime] AS VARCHAR(200)),19) AS VARCHAR(19)) AS  [DateAndTime]
		FROM 
			[MonitorLake].[dbo].[date]
		WHERE 
			[Date] = @Date
			AND 
			[IsWholeHour] = 1
		ORDER BY 
			1 desc
	END
	ELSE 
	/* audit-log */
	/* for the audit-logs - the extraction has to be at the day level - we extract the first hour of a day - depends on */
	BEGIN

		/* set the start and end date and time */
		DECLARE @StartDate DATE 
		DECLARE @EndDate DATE 

		/* set the start and end date */
		SET @EndDate = DATEADD(DAY, -1, CAST(GETDATE() AS DATE))				--end a day before
		SET @StartDate = DATEADD(DAY,-CAST(@AuditLogDays AS INT)+1, @EndDate)	--start n number of days before the end date where n = AuditLogDays
			
		--SELECT @StartDate,@EndDate

		SELECT 
			LEFT(CAST([DateAndTime] AS VARCHAR(200)),19) AS [DateAndTime]
		FROM 
			[MonitorLake].[dbo].[date]
		WHERE 
			[Date] BETWEEN @StartDate AND @EndDate
			AND 
			DATEDIFF(SECOND, [DateAndTime], CAST([Date] AS DATETIME2))= 0
		ORDER BY 
			1 desc


	END

END
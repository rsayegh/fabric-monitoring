CREATE PROCEDURE dbo.spGetLoad(
	 @ModuleRunId VARCHAR(200)
	,@Module VARCHAR(200)
)  
AS
BEGIN
	/*
		created by:			Raphael Sayegh

		Sample Call
		EXEC dbo.spGetLoad
			 @ModuleRunId = '9b112131-893e-4036-8044-47eee07735cb'
			,@Module = 'platform-monitoring'
	*/


	SELECT   
		MAX(LoadId) AS LoadId  
	FROM   
		dbo.Load  
	WHERE  
		ModuleRunId = @ModuleRunId  
		AND 
		[Module] = @Module

END
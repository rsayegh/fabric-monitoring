CREATE PROCEDURE dbo.spSelectNewTenantSettings
AS
BEGIN
	/*
		created by:			Raphael Sayegh

		Sample Call
		EXEC dbo.spSelectNewTenantSettings

	*/

	;WITH Datetimes AS(
		SELECT DISTINCT [Datetime]
		FROM [MonitorLake].[dbo].[tenant_settings]
	)
	,OrderedDatetimes AS(
		SELECT 
			[Datetime], ROW_NUMBER() OVER(ORDER BY [Datetime] DESC) AS RowNumber
		FROM 
			Datetimes
	)
	,CurrentSettings AS (
	SELECT 
		[SettingName]
		,[Title]
		,[Enabled]
		,[CanSpecifySecurityGroups]
		,[EnabledSecurityGroups]
		,[TenantSettingGroup]
		,[Properties]
		,ts.[Datetime]
	FROM 
		[MonitorLake].[dbo].[tenant_settings] ts
		INNER JOIN OrderedDatetimes odt
			ON odt.Datetime = ts.Datetime
	WHERE 
		odt.RowNumber = 1
	)
	,PreviousSettings AS(
	SELECT 
		[SettingName]
		,[Title]
		,[Enabled]
		,[CanSpecifySecurityGroups]
		,[EnabledSecurityGroups]
		,[TenantSettingGroup]
		,[Properties]
		,ts.[Datetime]
	FROM 
		[MonitorLake].[dbo].[tenant_settings] ts
		INNER JOIN OrderedDatetimes odt
			ON odt.Datetime = ts.Datetime
	WHERE 
		odt.RowNumber = 2
	)
	,NewSettings AS(
	SELECT 
		[SettingName]
		--,[Title]
		--,[Enabled]
		--,[CanSpecifySecurityGroups]
		--,[EnabledSecurityGroups]
		--,[TenantSettingGroup]
		--,[Properties]
	 --   ,[Datetime]
	FROM 
		CurrentSettings
	EXCEPT 
	SELECT 
		[SettingName]
		--,[Title]
		--,[Enabled]
		--,[CanSpecifySecurityGroups]
		--,[EnabledSecurityGroups]
		--,[TenantSettingGroup]
		--,[Properties]
	 --   ,[Datetime]
	FROM 
		PreviousSettings	
	)
	SELECT 
		CurrentSettings.[SettingName]
		,[Title]
		,[Enabled]
		,[CanSpecifySecurityGroups]
		,[EnabledSecurityGroups]
		,[TenantSettingGroup]
		,[Properties]
	    ,[Datetime]
	FROM 
		CurrentSettings 
		INNER JOIN NewSettings 
			ON NewSettings.[SettingName] = CurrentSettings.[SettingName]

			UNION ALL
	SELECT 
		CurrentSettings.[SettingName]
		,[Title]
		,[Enabled]
		,[CanSpecifySecurityGroups]
		,[EnabledSecurityGroups]
		,[TenantSettingGroup]
		,[Properties]
	    ,[Datetime]
	FROM 
		CurrentSettings 
		INNER JOIN NewSettings 
			ON NewSettings.[SettingName] = CurrentSettings.[SettingName]
	--WHERE CurrentSettings.[SettingName]= 'XXXX'
END
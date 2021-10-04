SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE   VIEW  [dbo].[Anios_SummaryDetails] 
WITH SCHEMABINDING AS
WITH ACT_DEM as (
	select CONCAT(A.[KEY], '*', CAST(A.[Date] as DATE)) as [Unique Id],  
	A.[KEY] as [Key],  CAST(A.[Date] as Date) as  [Date],  
	DATEPART(year, A.[Date]) as [Year],  DATEPART(month, A.[Date]) as [Month],  
	A.[Code Produit] as [Product Code],  A.[Sales Div] as [Sales Div],  A.[Désignation] as [Product Description], 
	A.[Division Mapping#Code Produit] as [Mother Division],  A.[Famille de produit] as [Material Type],  
	sum(
	case when A.[Actuals] = 0 and A.[Kg] >= 0 then A.[Kg] 
		 when A.[Actuals] > 0 then A.[Actuals] else A.[Actuals] end
	) as [Actual Demand],  sum(A.[Kg] ) as [Demand KG],
	NULL as [Actual Forecast], NULL as [Forecast Kg],
	NULL as [Model Forecast], NULL as [Comments],  NULL as [Forecast Accuracy], NULL as [Forecast Bias], 
	NULL as [User], 0.0 as [Forecast], A.[Delete_Ind]  as [DEL_IND]
	from dbo.Anios_DemandData A  
	where A.[Delete_Ind] = 'F'  and (datepart(year, CAST(SWITCHOFFSET(SYSDATETIMEOFFSET(), '+02:00') AS DATETIME)) - datepart(year, A.[Date])) <=3 
	group by A.[Key], A.[Date], A.[Code Produit], A.[Division Mapping#Code Produit],  A.[Famille de produit], 
	A.[Désignation], A.[Sales Div], A.[Delete_Ind]
),
ACT_FCST as (
	select CONCAT(A.[KEY], '*', CAST(A.[Date] as DATE)) as [Unique Id],  
	A.[KEY] as [Key],  CAST(A.[Date] as Date) as  [Date],  
	DATEPART(year, A.[Date]) as [Year],  DATEPART(month, A.[Date]) as [Month],  
	A.[Code Produit] as [Product Code],  A.[Sales Div] as [Sales Div],  A.[Désignation] as [Product Description], 
	A.[Division Mapping#Code Produit] as [Mother Division],  A.[Famille de produit] as [Material Type],  
	sum(
	case when A.Forecast = 0 and A.[Kg] >= 0 then A.[Kg] 
		 when A.Forecast > 0 then A.Forecast else A.Forecast end
	) as [Actual Forecast],  sum(A.[Kg] ) as [Forecast KG],
	NULL as [Actual Demand],  NULL as [Demand KG],
	NULL as [Model Forecast], NULL as [Comments],  NULL as [Forecast Accuracy], NULL as [Forecast Bias], 
	NULL as [User],  0.0 as [Forecast], A.[Delete_Ind]  as [DEL_IND]
	from dbo.Anios_ForecastData A  
	where A.[Delete_Ind] = 'F'  and (datepart(year, CAST(SWITCHOFFSET(SYSDATETIMEOFFSET(), '+02:00') AS DATETIME)) - datepart(year, A.[Date])) <=3 
	group by A.[Key], A.[Date], A.[Code Produit], A.[Division Mapping#Code Produit],  A.[Famille de produit], 
	A.[Désignation], A.[Sales Div], A.[Delete_Ind]
),
CAL_FCST as (
	select CONCAT(A.[KEY], '*', CAST(A.[Date] as DATE)) as [Unique Id],  
	A.[KEY] as [Key],  CAST(A.[Date] as Date) as  [Date],  
	DATEPART(year, A.[Date]) as [Year],  DATEPART(month, A.[Date]) as [Month],  
	A.[Code Produit] as [Product Code],  A.[Sales Div] as [Sales Div],  
	A.[Désignation] as [Product Description], 
	A.[Division Mapping#Code Produit] as [Mother Division],  A.[Famille de produit] as [Material Type],  
	NULL as [Actual Demand],  NULL as [Demand KG],
	NULL as [Actual Forecast],  NULL as [Forecast KG],
	A.[StatForecast] as [Model Forecast], A.[Comments] as [Comments],  A.[Forecast Accuracy] as [Forecast Accuracy], 
	A.[Forecast Bias] as [Forecast Bias], 
	A.[Username] as [User],  A.[Forecast] as [Forecast], A.[Delete_Indicator]  as [DEL_IND]
	from dbo.Anios_CalForecastData A
	where A.[Delete_Indicator] = 'F'  
), 
FCST_SUMMARY as(
	select IsNull(ACT_FCST.[Unique Id], CAL_FCST.[Unique Id]) as [Unique Id], 
	IsNull(ACT_FCST.[KEY], CAL_FCST.[Key])  as [Key],  
	IsNull(ACT_FCST.[Date],CAL_FCST.[Date])  as [Date],  
	IsNull(ACT_FCST.[Year], CAL_FCST.[Year]) as [Year], 
	IsNull(ACT_FCST.[Month], CAL_FCST.[Month]) as [Month],  
	IsNull(ACT_FCST.[Product Code], CAL_FCST.[Product Code] ) as [Product Code],  
	IsNull(ACT_FCST.[Sales Div],CAL_FCST.[Sales Div] ) as [Sales Div], 
	IsNull(ACT_FCST.[Product Description], CAL_FCST.[Product Description]) as [Product Description], 
	IsNull(ACT_FCST.[Mother Division],CAL_FCST.[Mother Division] ) as [Mother Division],  
	IsNull(ACT_FCST.[Material Type], CAL_FCST.[Material Type]) as [Material Type] , 
	Null as [Actual Demand], Null as [Demand KG],
	IsNull(ACT_FCST.[Actual Forecast], 0.0) as [Actual Forecast],  IsNull(ACT_FCST.[Forecast KG], 0.0) as [Forecast KG],
	IsNull( IsNull(ACT_FCST.[Model Forecast], CAL_FCST.[Model Forecast]), 0.0) as [Model Forecast], 
	IsNull( IsNull(ACT_FCST.[Comments],CAL_FCST.[Comments]), '') as [Comments],  
	Case when IsNull((ABS(IsNull(CAL_FCST.[Model Forecast],0) - IsNull(ACT_FCST.[Actual Forecast],0))/(NULLIF(ACT_FCST.[Actual Forecast],0))),0) > 1 then 0.0 
	else  1 - IsNull((ABS(IsNull(CAL_FCST.[Model Forecast],0) - IsNull(ACT_FCST.[Actual Forecast],0))/(NULLIF(ACT_FCST.[Actual Forecast],0))),0) end 
	as [Forecast Accuracy], 
	Isnull(((IsNull(CAL_FCST.[Model Forecast],0) - IsNull(ACT_FCST.[Actual Forecast],0))/(NULLIF(ACT_FCST.[Actual Forecast],0))),0) as [Forecast Bias], 
	IsNull( IsNull(ACT_FCST.[User],CAL_FCST.[User]), '') as [User],  IsNull(CAL_FCST.[Forecast],0.0) as [Forecast], 
	CAST(SWITCHOFFSET(SYSDATETIMEOFFSET(), '+02:00') AS DATETIME) as [Update_timestamp], 
	IsNull(ACT_FCST.[DEL_IND], CAL_FCST.[DEL_IND]) as [Delete_Ind]
	from CAL_FCST 
	full outer join ACT_FCST on CAL_FCST.[Unique Id] = ACT_FCST.[Unique Id]
), 
SUMMARY as ( 
	select 
	IsNull(ACT_DEM.[Unique Id], FCST_SUMMARY.[Unique Id]) as [Unique Id],
	IsNull(ACT_DEM.[Key], FCST_SUMMARY.[Key]) as [Key],
	IsNull(ACT_DEM.[Date], FCST_SUMMARY.[Date]) as [Date],
	IsNull(ACT_DEM.[Year], FCST_SUMMARY.[Year]) as [Year],
	IsNull(ACT_DEM.[Month], FCST_SUMMARY.[Month]) as [Month],  
	IsNull(ACT_DEM.[Product Code], FCST_SUMMARY.[Product Code] ) as [Product Code],  
	IsNull(ACT_DEM.[Sales Div],FCST_SUMMARY.[Sales Div] ) as [Sales Div], 
	IsNull(ACT_DEM.[Product Description], FCST_SUMMARY.[Product Description]) as [Product Description], 
	IsNull(ACT_DEM.[Mother Division],FCST_SUMMARY.[Mother Division] ) as [Mother Division],  
	IsNull(ACT_DEM.[Material Type], FCST_SUMMARY.[Material Type]) as [Material Type] , 
	IsNull(ACT_DEM.[Actual Demand], 0.0)  as [Actual Demand], IsNull(ACT_DEM.[Demand KG], 0.0)  as [Demand KG],
	IsNull(IsNull(ACT_DEM.[Actual Forecast], FCST_SUMMARY.[Actual Forecast]), 0.0) as [Actual Forecast],  
	IsNull(IsNull(ACT_DEM.[Forecast KG], FCST_SUMMARY.[Forecast KG]), 0.0) as [Forecast KG],
	IsNull(IsNull(ACT_DEM.[Model Forecast], FCST_SUMMARY.[Model Forecast]), 0.0) as [Model Forecast], 
	IsNull(FCST_SUMMARY.[Comments],'') as [Comments],  
	IsNull(IsNull(ACT_DEM.[Forecast Accuracy], FCST_SUMMARY.[Forecast Accuracy]), 0.0) as [Forecast Accuracy], 
	IsNull(IsNull(ACT_DEM.[Forecast Bias], FCST_SUMMARY.[Forecast Bias]), 0.0) as [Forecast Bias], 
	IsNUll(FCST_SUMMARY.[User],'') as [User],  
	IsNull(FCST_SUMMARY.[Forecast], 0) as [Final Forecast], 
	CAST(SWITCHOFFSET(SYSDATETIMEOFFSET(), '+02:00') AS DATETIME) as [Update_timestamp], 
	IsNull(ACT_DEM.[DEL_IND], FCST_SUMMARY.[Delete_Ind]) as [Delete_Ind]
	from FCST_SUMMARY full outer join ACT_DEM on ACT_DEM.[Unique Id] = FCST_SUMMARY.[Unique Id]
)
select newid() as [id], 
	[Unique Id],
	[Key],
	[Date],
	[Year],
	[Month],  
	[Product Code],  
	[Sales Div], 
	[Product Description], 
	[Mother Division],  
	[Material Type] , 
	[Actual Demand],
	[Demand KG],
	[Actual Forecast],  
	[Forecast KG],
	[Model Forecast], 
	[Comments],  
	[Forecast Accuracy], 
	[Forecast Bias], 
	[User],  
	[Final Forecast], 
	[Update_timestamp], 
	[Delete_Ind]
from SUMMARY 
GO



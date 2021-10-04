/****** Object:  Table [dbo].[Anios_CalForecastData]    Script Date: 7/16/2021 4:46:59 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[Anios_CalForecastData](
	[id] [nvarchar](255) NOT NULL,
	[Code Produit] [nvarchar](255) NULL,
	[Désignation] [nvarchar](255) NULL,
	[Division Mapping#Code Produit] [nvarchar](255) NULL,
	[Famille de produit] [nvarchar](255) NULL,
	[Date] [datetime] NULL,
	[StatForecast] [float] NULL,
	[Forecast] [float] NULL,
	[Key] [nvarchar](255) NOT NULL,
	[Update_Timestamp] [datetime] NULL,
	[Delete_Indicator] [nvarchar](1) NOT NULL,
	[KG] [float] NULL,
	[Comments] [nvarchar](255) NULL,
	[Sales Div] [nvarchar](255) NULL,
	[Forecast Accuracy] [float] NULL,
	[Forecast Bias] [float] NULL,
	[Username] [nvarchar](255) NULL,
PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[Anios_CalForecastData] ADD  DEFAULT ('0.0') FOR [Forecast]
GO

ALTER TABLE [dbo].[Anios_CalForecastData] ADD  DEFAULT (getdate()) FOR [Update_Timestamp]
GO

ALTER TABLE [dbo].[Anios_CalForecastData] ADD  DEFAULT ('F') FOR [Delete_Indicator]
GO



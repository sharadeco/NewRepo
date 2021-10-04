/****** Object:  Table [dbo].[Anios_DemandData]    Script Date: 7/16/2021 4:47:52 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[Anios_DemandData](
	[id] [nvarchar](255) NOT NULL,
	[Code Produit] [nvarchar](255) NULL,
	[Désignation] [nvarchar](255) NULL,
	[Division Mapping#Code Produit] [nvarchar](255) NULL,
	[Famille de produit] [nvarchar](255) NULL,
	[Date] [datetime] NULL,
	[Actuals] [float] NULL,
	[key] [nvarchar](255) NOT NULL,
	[Update_Timestamp] [datetime] NULL,
	[KG] [float] NULL,
	[Delete_Ind] [nvarchar](1) NULL,
	[Sales Div] [nvarchar](255) NULL,
PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[Anios_DemandData] ADD  DEFAULT (getdate()) FOR [Update_Timestamp]
GO


CREATE TABLE [dbo].[CommonCustomerVehicle](
	[PKCommonCustomerVehicleId] [int] IDENTITY(1,1) NOT NULL,
	[FKCustomerID] [varchar](50) NULL,
	[VehicleDateCreated] [datetime] NOT NULL,
	[FKImportLogID] [int] NOT NULL,
	[Sales1UserID] [varchar](50) NULL,
	[FKDealID] [varchar](50) NULL,
	[NewUsedType] [int] NOT NULL,
	[InterestType] [int] NOT NULL,
	[Year] [int] NULL,
	[Make] [varchar](64) NULL,
	[Model] [varchar](64) NULL,
	[Trim] [varchar](64) NULL,
	[OdometerStatus] [int] NULL,
	[InteriorColor] [varchar](64) NULL,
	[ExteriorColor] [varchar](64) NULL,
	[VIN] [varchar](17) NULL,
	[StockNumber] [varchar](64) NULL,
	[DateModified] [datetime] NULL,
 CONSTRAINT [PK_CommonCustomerVehicle] PRIMARY KEY CLUSTERED 
(
	[PKCommonCustomerVehicleId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[CommonCustomerVehicle] ADD  DEFAULT ('') FOR [InteriorColor]
GO

ALTER TABLE [dbo].[CommonCustomerVehicle] ADD  DEFAULT ('') FOR [ExteriorColor]
GO

CREATE NONCLUSTERED INDEX [ix_CommonCustomerVehicle_FKImportLogID_FKDealID] ON [dbo].[CommonCustomerVehicle] ([FKImportLogID], [FKDealID]) INCLUDE ([InterestType])
GO
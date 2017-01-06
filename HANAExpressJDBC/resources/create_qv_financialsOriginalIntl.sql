USE [EnterpriseReporting]
GO

/****** Object:  View [QV].[FinancialsOriginalIntl]    Script Date: 06.01.2017 10:07:11 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE VIEW [QV].[FinancialsOriginalIntl] AS
SELECT [DRWSK]
      ,F.[CreateDate]
      ,F.[ModifyDate]
      ,F.[DataSourceRef]
      ,[AccountSK]
      ,[AccountPostingNumber]
      ,[CompanySK]
      ,[CedentSK]
      ,F.[TreatySK]
      ,[TreatyPeriodSK]
      ,[TreatySectionSK]
      ,[TreatyInvolvementSK]
      ,[AccountingUnitSK]
      ,[LineOfBusinessSK]
      ,[LossSK]
      ,[ActivitySK]
      ,[ClaimObjectSK]
      ,[EstimateSK]
      ,EC.[EntryCodeSK]
      ,[PostingRecordSK]
      ,[UnderwritingAreaSK]
      ,[TreatyHistorySK]
      ,[TreatySectionHistorySK]
      ,[TreatyInvolvementHistorySK]
      ,[BrokerSK]
      ,[IBNRReserveCellSK]
      ,[MCOBSK]
      ,[LineOfBusinessCode]
      ,[AccountStatusCode]
      ,[AccountPeriodEndDate]
      ,[UnderwritingDate]
      ,[OccurrenceDate]
      ,[FIPostingDate]
      ,[FinancialPeriod]
      ,[AccountingPeriod]
      ,[AccountingYear]
      ,[UnderwritingYear]
      ,[GenReUnderwritingYear]
      ,[OccurrenceYear]
      ,[OriginalCurrencySK]
	  ,[OriginalCurrency]
      ,(F.OriginalAmount*EC.Operator) as [OriginalAmount]
  FROM [EnterpriseWarehouse].[dbo].[EW_F_DRW] F
    INNER JOIN [EnterpriseWarehouse].[dbo].[EW_O_Treaty] T
    ON F.TreatySK = T.TreatySK AND T.LegalEntity in ('1', '12','-99')
  INNER JOIN [EnterpriseWarehouse].[dbo].[EW_O_EntryCode] EC
  ON F.EntryCodeSK = EC.EntryCodeSK 

GO


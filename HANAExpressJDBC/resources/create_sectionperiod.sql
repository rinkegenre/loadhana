CREATE COLUMN TABLE EOC.EO_O_GTFESectionPeriod 
 (SectionPeriodGuid     VARCHAR (22)     NOT NULL,
    DeleteFlag            SMALLINT        NOT NULL,
    CreateDate            DATETIME        NOT NULL,
    CreateUser            NVARCHAR (32)   NOT NULL,
    ModifyDate            DATETIME        NOT NULL,
    ModifyUser            NVARCHAR (32)   NOT NULL,
    DataSourceRef         NVARCHAR (32)   NOT NULL,
    TreatyGuid            VARCHAR (22) NOT NULL,
    SectionGuid           VARCHAR (22) NOT NULL,
    PeriodGuid            VARCHAR (22) NOT NULL,
    SAPChangeDate         DATETIME        NULL,
    MainCurrency          NVARCHAR (5)    NULL,
    ExchangeRateType      NVARCHAR (4)    NULL,
    LeadReinsurer         NVARCHAR (5)    NULL,
    ECOPercentage         DECIMAL (13, 9) NULL,
    ECOPercentageType     CHAR (1)        NULL,
    XPLPercentage         DECIMAL (13, 9) NULL,
    DJExpencesPercentage  DECIMAL (13, 9) NULL,
    IntlBusinessIndicator CHAR (1)        NULL,
    NatCatEventExposed    CHAR (1)        NULL,
    Earthquake            CHAR (1)        NULL,
    Wind                  CHAR (1)        NULL,
    Flood                 CHAR (1)        NULL,
    Hail                  CHAR (1)        NULL,
    Terrorism             CHAR (1)        NULL,
    LayerTypeCode         NVARCHAR (3)    NULL,
    TransferredUser       NVARCHAR (12)   NULL,
    TransferredDate       DATE            NULL,
    TransferredTime       NVARCHAR (6)    NULL,
    ActiveSection         CHAR (1)        NULL,
    TransferredIndicator  CHAR (1))
	
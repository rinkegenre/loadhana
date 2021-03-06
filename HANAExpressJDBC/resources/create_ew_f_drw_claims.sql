CREATE column TABLE eoc.EW_F_DRW_Claims (
    ClaimsSK              BIGINT          NOT NULL,
    DeleteFlag            SMALLINT        NOT NULL,
    CreateDate            DATETIME        NOT NULL,
    CreateUser            NVARCHAR (32)   NOT NULL,
    ModifyDate            DATETIME        NOT NULL,
    ModifyUser            NVARCHAR (32)   NOT NULL,
    DataSource            NVARCHAR (32)   NOT NULL,
    DataSourceRef         NVARCHAR (32)   NOT NULL,   
    Treaty                NVARCHAR (20)   NULL,
    TreatyPeriodStartDate DATE            NULL,
    Section               NVARCHAR (4)    NULL,
    Loss                  NVARCHAR (10)   NULL,
    Activity              NVARCHAR (20)   NULL,
    ClaimObject           NVARCHAR (20)   NULL,
    FIPostingDate         DATE            NOT NULL,
    FinancialPeriod       INT             NOT NULL,
    UnderwritingYear      SMALLINT        NOT NULL,
    GenReUnderwritingYear SMALLINT        NOT NULL,
    OriginalCurrency      NVARCHAR (5)    NOT NULL,
    AccountingCurrency    NVARCHAR (5)    NOT NULL,
    TransactionCurrency   NVARCHAR (5)    NOT NULL,
    CompanyCurrency       NVARCHAR (5)    NOT NULL,
    HomeCurrency          NVARCHAR (5)    NOT NULL,
    GroupCurrency         NVARCHAR (5)    NOT NULL,
    OriginalAmount        DECIMAL (24, 4) NOT NULL,
    AccountingAmount      DECIMAL (24, 4) NOT NULL,
    TransactionAmount     DECIMAL (24, 4) NOT NULL,
    CompanyAmount         DECIMAL (24, 4) NOT NULL,
    HomeAmount            DECIMAL (24, 4) NOT NULL,
    GroupAmount           DECIMAL (24, 4) NOT NULL
);



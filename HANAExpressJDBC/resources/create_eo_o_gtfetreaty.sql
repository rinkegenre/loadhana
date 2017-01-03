CREATE column TABLE EO_O_GTFETreaty (
    TreatyGuid              VARCHAR (22)  ,
    DeleteFlag              SMALLINT      NOT NULL,
    CreateDate              DATETIME      NOT NULL,
    CreateUser              NVARCHAR (32) NOT NULL,
    ModifyDate              DATETIME      NOT NULL,
    ModifyUser              NVARCHAR (32) NOT NULL,
    DataSourceRef           NVARCHAR (32) NOT NULL,
    Treaty                  NVARCHAR (20) NULL,
    TreatyDesc              NVARCHAR (40) NULL,
    NatureOfTreaty          CHAR (1)      NULL,
    Company                 NVARCHAR (4)  NULL,
    Cedent                  NVARCHAR (10) NULL,
    TreatyEffectiveFromDate DATETIME      NULL,
    TreatyEffectiveToDate   DATETIME      NULL,
    MasterContract          NVARCHAR (20) NULL,
    SAPChangeDate           DATETIME      NULL,
    ReinsuranceCharacter    NVARCHAR (2)  NULL,
    BUAccoutability         NVARCHAR (8)  NULL,
    Division                NVARCHAR (8)  NULL
);



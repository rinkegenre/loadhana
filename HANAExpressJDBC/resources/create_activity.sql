// comment
CREATE column TABLE eoc.EW_O_Activity (
    ActivitySK         INT           NOT NULL,
    DeleteFlag         SMALLINT      NOT NULL,
    CreateDate         DATETIME      NOT NULL,
    CreateUser         NVARCHAR (32) NOT NULL,
    ModifyDate         DATETIME      NOT NULL,
    ModifyUser         NVARCHAR (32) NOT NULL,
    DataSourceRef      NVARCHAR (32) NOT NULL,
    Activity           NVARCHAR (20) NULL,
    ActivityDesc       NVARCHAR (40) NULL,
    ActivityStatus     NVARCHAR (4)  NULL,
    MLASuppressionFlag CHAR (1)      NULL,
    LeadingLoss        NVARCHAR (10) NULL,
    LeadingLossSK      INT           NOT NULL,
    ActivityStatusDesc NVARCHAR (40) NULL,
    DateOfReceipt      DATE          NULL
);
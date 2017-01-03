CREATE COLUMN TABLE EOC.EO_O_GTFEPeriodRIP 
	("SectionPeriodGuid" NVARCHAR(22) NOT NULL ,
	 "ReinsuranceProgram" NVARCHAR(4) NOT NULL ,
	 "DeleteFlag" SMALLINT CS_INT,
	 "CreateDate" LONGDATE CS_LONGDATE NOT NULL ,
	 "CreateUser" NVARCHAR(32),
	 "ModifyDate" LONGDATE CS_LONGDATE NOT NULL ,
	 "ModifyUser" NVARCHAR(32),
	 "DataSourceRef" NVARCHAR(32),
	 "ReinsuranceProgramDesc" NVARCHAR(40),
	 "TreatyGuid" NVARCHAR(22),
	 "SectionGuid" NVARCHAR(22),
	 "PeriodGuid" NVARCHAR(22)) ;
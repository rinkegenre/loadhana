package com.genre.hana.load;

import java.sql.*;

public class LoadEOCaccount {
	public static void main(String[] argv) {
		Connection hanaConnection = null;
		Connection mssqlConnection = null;
		try {
			hanaConnection = DriverManager.getConnection( // 10.173.19.184
					"jdbc:sap://hxehost.grn.genre.com:30013/?autocommit=true?databaseName=SYSTEM&user=SYSTEM&password=Elster2016",
					"SYSTEM", "Elster2016");

			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");

			mssqlConnection = DriverManager.getConnection(
					"jdbc:sqlserver://USTRNTQD326.grn.genre.com:1433;databaseName=EnterpriseWarehouse;user=SAPPI;password=SAPPI1234");

		} catch (SQLException e) {

			System.err.print(e.getMessage());
			return;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		if (hanaConnection != null & mssqlConnection != null) {
			PreparedStatement pstmt = null;
			try {
				java.util.Date date = new java.util.Date(); 
				System.out.println("Connection to HANA & MSSQL successful!" + date.toGMTString() );
				pstmt = hanaConnection.prepareStatement(
						"INSERT INTO EOC.EW_O_ACCOUNTC"
						+ "(AccountSK,DeleteFlag, CreateDate,  CreateUser, ModifyDate, ModifyUser, DataSourceRef,ActivitySK, Account, AccountDesc, AccountStatus,"
						+  " AccountStatusCode, AccountStatusDesc, AccountPeriodStartDate, AccountPeriodEndDate, AccountingYear,AccountReleaseDate, AccountCedent,"          
						+   " AccountCedentDesc,AccountReceiptDate, AccountDueDate, AccountCreateDate, LogicalSystem,PartnersInvolved, PartnersInvolvedDesc,"
						+    "ExternalPartner, ExternalPartnerDesc,AccountFunction, AccountOrigin, AccountFunctionDesc, AccountOriginDesc, ReleaseDate,"
						+    "OriginalCedent, OriginalCedentDesc, AccountCreateTime,AccountReleaseTime , AccountingPeriod, AccountingPeriodDesc,Treaty,"
						+    "Section,TransactionType ,BusinessType,AvsEAllocationPeriod, AccountOverdueBand ,CAPLogComplete,ProvisionalEntry ,  AccountingFrequency"
						+ ")"
				      + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
				pstmt.setPoolable(true);

				Statement mssql_stmt = mssqlConnection.createStatement();
				ResultSet mssql_resultSet = mssql_stmt.executeQuery("select * from [dbo].[EW_O_Account] order by AccountSK desc");
				long rowno = 0;
				int batchno = 1;
				while (mssql_resultSet.next()) {

					rowno++;

					pstmt.setLong(1, mssql_resultSet.getLong(1));
					pstmt.setInt(2, mssql_resultSet.getInt(2));
					pstmt.setTimestamp(3, mssql_resultSet.getTimestamp(3));
					pstmt.setString(4, mssql_resultSet.getString(1));
					pstmt.setTimestamp(5, mssql_resultSet.getTimestamp(5));
					pstmt.setString(6, mssql_resultSet.getString(6));					
					pstmt.setString(7, mssql_resultSet.getString(7));					
					pstmt.setInt(8, mssql_resultSet.getInt(8));					
					pstmt.setString(9, mssql_resultSet.getString(9));
					pstmt.setString(10, mssql_resultSet.getString(10));					
					pstmt.setString(11, mssql_resultSet.getString(11));					
					pstmt.setString(12, mssql_resultSet.getString(12));
					pstmt.setString(13, mssql_resultSet.getString(13));
					pstmt.setTimestamp(14, mssql_resultSet.getTimestamp(14));
					pstmt.setTimestamp(15, mssql_resultSet.getTimestamp(15));
					pstmt.setInt(16, mssql_resultSet.getInt(16));
					pstmt.setTimestamp(17, mssql_resultSet.getTimestamp(17));
					pstmt.setString(18, mssql_resultSet.getString(18));
					pstmt.setString(19, mssql_resultSet.getString(19));															
					pstmt.setTimestamp(20, mssql_resultSet.getTimestamp(20));
					pstmt.setTimestamp(21, mssql_resultSet.getTimestamp(21));
					pstmt.setTimestamp(22, mssql_resultSet.getTimestamp(22));					
					pstmt.setString(23, mssql_resultSet.getString(23));
					pstmt.setString(24, mssql_resultSet.getString(24));
					pstmt.setString(25, mssql_resultSet.getString(25));
					pstmt.setString(26, mssql_resultSet.getString(26));
					pstmt.setString(27, mssql_resultSet.getString(27));
					pstmt.setString(28, mssql_resultSet.getString(28));
					pstmt.setString(29, mssql_resultSet.getString(29));
					pstmt.setString(30, mssql_resultSet.getString(30));
					pstmt.setString(31, mssql_resultSet.getString(31));
					
					pstmt.setTimestamp(32, mssql_resultSet.getTimestamp(32));
					pstmt.setString(33, mssql_resultSet.getString(33));
					pstmt.setString(34, mssql_resultSet.getString(34));
					pstmt.setTimestamp(35, mssql_resultSet.getTimestamp(35));
					pstmt.setTimestamp(36, mssql_resultSet.getTimestamp(36));
					pstmt.setString(37, mssql_resultSet.getString(37));
					pstmt.setString(38, mssql_resultSet.getString(38));
					pstmt.setString(39, mssql_resultSet.getString(39));
					pstmt.setString(40, mssql_resultSet.getString(40));
					pstmt.setString(41, mssql_resultSet.getString(41));
					pstmt.setString(42, mssql_resultSet.getString(42));
					pstmt.setString(43, mssql_resultSet.getString(43));
					pstmt.setString(44, mssql_resultSet.getString(44));
					pstmt.setString(45, mssql_resultSet.getString(45));
					pstmt.setString(46, mssql_resultSet.getString(46));
					pstmt.setString(47, mssql_resultSet.getString(47));
					
					int rowcnt = pstmt.executeUpdate();
					if (batchno++ > 1000)
					{
						hanaConnection.commit();
						System.out.println("rows inserted " + rowno);
						batchno= 0;
					}

					if (rowcnt != 1) {
						System.out.println("no row inserted for rowno " + rowno);
						break;
					}
				
				};
				hanaConnection.commit();
				
				date = new java.util.Date();
				System.out.println("rows inserted " + rowno + "" +  date.toGMTString());
/*				if (rowno == 100)
				{	
					System.out.println("rows inserted " + rowno);
					break;
				}*/
				
				/*
				 * TreatyGuid VARCHAR (22) , DeleteFlag SMALLINT NOT NULL,
				 * CreateDate DATETIME NOT NULL, CreateUser NVARCHAR (32) NOT
				 * NULL, ModifyDate DATETIME NOT NULL, ModifyUser NVARCHAR (32)
				 * NOT NULL, DataSourceRef NVARCHAR (32) NOT NULL, Treaty
				 * NVARCHAR (20) NULL, TreatyDesc NVARCHAR (40) NULL,
				 * NatureOfTreaty CHAR (1) NULL, Company NVARCHAR (4) NULL,
				 * Cedent NVARCHAR (10) NULL, TreatyEffectiveFromDate DATETIME
				 * NULL, TreatyEffectiveToDate DATETIME NULL, MasterContract
				 * NVARCHAR (20) NULL, SAPChangeDate DATETIME NULL,
				 * ReinsuranceCharacter NVARCHAR (2) NULL, BUAccoutability
				 * NVARCHAR (8) NULL, Division NVARCHAR (8) NULL
				 */
				

			} catch (SQLException e) {				
				e.printStackTrace();
			} finally {

				try {
					pstmt.close();
					hanaConnection.close();
					mssqlConnection.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
}

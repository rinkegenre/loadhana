package com.genre.hana.load;

import java.sql.*;

public class LoadEW_F_DRW_Claims {
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
						"INSERT INTO eoc.EW_F_DRW_Claims"
						+ "(ClaimsSK,DeleteFlag, CreateDate, CreateUser, ModifyDate,ModifyUser, DataSource, DataSourceRef,"
						+ "Treaty, TreatyPeriodStartDate,Section, Loss, Activity,ClaimObject, FIPostingDate, FinancialPeriod, UnderwritingYear, GenReUnderwritingYear," 
    + "OriginalCurrency, AccountingCurrency,TransactionCurrency,CompanyCurrency, HomeCurrency,"
    + "GroupCurrency, OriginalAmount, AccountingAmount, TransactionAmount, CompanyAmount, HomeAmount,  GroupAmount"
						+ ")"
				      + "VALUES (?,?,?,?,?,?,?,?,?,?,"
						+ "?,?,?,?,?,?,?,?,?,?,"
						+ "?,?,?,?,?,?,?,?,?,?)"
				      );
				pstmt.setPoolable(true);

				Statement mssql_stmt = mssqlConnection.createStatement();
				
				ResultSet mssql_resultSet = mssql_stmt.executeQuery(
						"select  ClaimsSK,DeleteFlag, CreateDate, CreateUser, ModifyDate,ModifyUser, DataSource, DataSourceRef, Treaty,"
 + "TreatyPeriodStartDate,Section, Loss, Activity,ClaimObject, FIPostingDate, FinancialPeriod, UnderwritingYear, GenReUnderwritingYear,"
 + "OriginalCurrency, AccountingCurrency,TransactionCurrency,CompanyCurrency, HomeCurrency,"
 + "GroupCurrency, OriginalAmount, AccountingAmount, TransactionAmount, CompanyAmount, HomeAmount,  GroupAmount"
 + " from [dbo].[EW_F_DRW_Claims] order by ClaimsSK desc");
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
					pstmt.setString(8, mssql_resultSet.getString(8));	
					
					pstmt.setString(9, mssql_resultSet.getString(9));
					pstmt.setDate(10, mssql_resultSet.getDate(10));					
					pstmt.setString(11, mssql_resultSet.getString(11));					
					pstmt.setString(12, mssql_resultSet.getString(12));
					pstmt.setString(13, mssql_resultSet.getString(13));
					pstmt.setString(14, mssql_resultSet.getString(14));
					pstmt.setDate(15, mssql_resultSet.getDate(15));
					pstmt.setInt(16, mssql_resultSet.getInt(16));
					pstmt.setInt(17, mssql_resultSet.getInt(17));
					pstmt.setInt(18, mssql_resultSet.getInt(18));
					pstmt.setString(19, mssql_resultSet.getString(19));															
					pstmt.setString(20, mssql_resultSet.getString(20));
					pstmt.setString(21, mssql_resultSet.getString(21));
					pstmt.setString(22, mssql_resultSet.getString(22));					
					pstmt.setString(23, mssql_resultSet.getString(23));
					pstmt.setString(24, mssql_resultSet.getString(24));
					pstmt.setBigDecimal(25, mssql_resultSet.getBigDecimal(25));
					pstmt.setBigDecimal(26, mssql_resultSet.getBigDecimal(26));
					pstmt.setBigDecimal(27, mssql_resultSet.getBigDecimal(27));
					pstmt.setBigDecimal(28, mssql_resultSet.getBigDecimal(28));
					pstmt.setBigDecimal(29, mssql_resultSet.getBigDecimal(29));
					pstmt.setBigDecimal(30, mssql_resultSet.getBigDecimal(30));					
					
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

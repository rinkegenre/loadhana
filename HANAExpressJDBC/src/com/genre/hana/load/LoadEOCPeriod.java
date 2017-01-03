package com.genre.hana.load;

import java.sql.*;

public class LoadEOCPeriod {
	public static void main(String[] argv) {
		Connection hanaConnection = null;
		Connection mssqlConnection = null;
		try {
			hanaConnection = DriverManager.getConnection( // 10.173.19.184
					"jdbc:sap://hxehost.grn.genre.com:30013/?autocommit=true?databaseName=SYSTEM&user=SYSTEM&password=Elster2016",
					"SYSTEM", "Elster2016");

			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");

			mssqlConnection = DriverManager.getConnection(
					"jdbc:sqlserver://USTRNTQD326.grn.genre.com:1433;databaseName=EnterpriseOC;user=SAPPI;password=SAPPI1234");

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
						"INSERT INTO EOC.EO_O_GTFEPERIOD "
						+ "(PeriodGuid, DeleteFlag,"
					    + "CreateDate, CreateUser,  ModifyDate,ModifyUser, DataSourceRef, TreatyGuid,"
						+ "TreatyPeriodStartDate,TreatyPeriodEndDate, ProcessStatus,SAPChangeDate,"
					    + "ReferralCategory, FSRIPeriodStatus, BusinessStatus,OutcomeStatus,UnderwritingStatus,"
					    + "GuidelineStatus, GuidelineGuid, UnderwritingGuid, MarketingGuid, AnniversaryDate, ContractRequestDate) "
				      + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
				

				Statement mssql_stmt = mssqlConnection.createStatement();
				ResultSet mssql_resultSet = mssql_stmt.executeQuery("select * from [TTY].[EO_O_GTFEPeriod]");
				long rowno = 0;
				int batchno = 1;
				while (mssql_resultSet.next()) {

					rowno++;

					pstmt.setString(1, mssql_resultSet.getString(1)); //key
				
					pstmt.setInt(2, mssql_resultSet.getInt(2)); // prefix
					pstmt.setDate(3, mssql_resultSet.getDate(3));
					pstmt.setString(4, mssql_resultSet.getString(1));
					pstmt.setDate(5, mssql_resultSet.getDate(5));
					pstmt.setString(6, mssql_resultSet.getString(6));					
				    pstmt.setString(7, mssql_resultSet.getString(7));					
					pstmt.setString(8, mssql_resultSet.getString(8));
					
					pstmt.setDate(9, mssql_resultSet.getDate(9));
					pstmt.setDate(10, mssql_resultSet.getDate(10));
					pstmt.setString(11, mssql_resultSet.getString(11));
					
					pstmt.setDate(12, mssql_resultSet.getDate(12));
					pstmt.setString(13, mssql_resultSet.getString(13));
					pstmt.setString(14, mssql_resultSet.getString(14));
					pstmt.setString(15, mssql_resultSet.getString(15));
					pstmt.setString(16, mssql_resultSet.getString(16));
					pstmt.setString(17, mssql_resultSet.getString(17));
					pstmt.setString(18, mssql_resultSet.getString(18));
					pstmt.setString(19, mssql_resultSet.getString(19));
															
					pstmt.setString(20, mssql_resultSet.getString(20));
					pstmt.setString(21, mssql_resultSet.getString(21));
					pstmt.setDate(22, mssql_resultSet.getDate(22));
					pstmt.setDate(23, mssql_resultSet.getDate(23));
					
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

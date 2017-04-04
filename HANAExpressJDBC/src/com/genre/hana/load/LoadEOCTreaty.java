package com.genre.hana.load;

import java.sql.*;

public class LoadEOCTreaty {
	public static void main(String[] argv) {
		Connection hanaConnection = null;
		Connection mssqlConnection = null;
		try {
			hanaConnection = DriverManager.getConnection( // 10.173.19.184
					"jdbc:sap://hxehost.grn.genre.com:39013/?databaseName=HDRW&autocommit=false",
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
						"INSERT INTO EOC.EO_O_GTFETreaty "
						+ "(TreatyGuid, DeleteFlag, CreateDate,CreateUser,ModifyDate,ModifyUser,DataSourceRef,Treaty, TreatyDesc, NatureOfTreaty, Company, Cedent,TreatyEffectiveFromDate, TreatyEffectiveToDate, MasterContract, SAPChangeDate,ReinsuranceCharacter, BUAccoutability, Division) "
						+ "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");

				Statement mssql_stmt = mssqlConnection.createStatement();
				ResultSet mssql_resultSet = mssql_stmt.executeQuery("select * from [TTY].[EO_O_GTFETREATY]");
				long rowno = 0;
				int batchno = 1;
				while (mssql_resultSet.next()) {

					rowno++;

					pstmt.setString(1, mssql_resultSet.getString(1));
					pstmt.setInt(2, mssql_resultSet.getInt(2));
					pstmt.setDate(3, mssql_resultSet.getDate(3));
					pstmt.setString(4, mssql_resultSet.getString(1));
					pstmt.setDate(5, mssql_resultSet.getDate(5));
					pstmt.setString(6, mssql_resultSet.getString(6));
					pstmt.setString(7, mssql_resultSet.getString(7));
					pstmt.setString(8, mssql_resultSet.getString(8));
					pstmt.setString(9, mssql_resultSet.getString(9));
					pstmt.setString(10, mssql_resultSet.getString(10));
					pstmt.setString(11, mssql_resultSet.getString(11));
					pstmt.setString(12, mssql_resultSet.getString(12));
					pstmt.setDate(13, mssql_resultSet.getDate(13));
					pstmt.setDate(14, mssql_resultSet.getDate(14));
					pstmt.setString(15, mssql_resultSet.getString(15));
					pstmt.setDate(16, mssql_resultSet.getDate(16));
					pstmt.setString(17, mssql_resultSet.getString(17));
					pstmt.setString(18, mssql_resultSet.getString(18));
					pstmt.setString(19, mssql_resultSet.getString(19));
					
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

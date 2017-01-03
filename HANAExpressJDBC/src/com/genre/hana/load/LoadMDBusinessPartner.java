package com.genre.hana.load;

import java.sql.*;

public class LoadMDBusinessPartner {
	public static void main(String[] argv) {
		Connection hanaConnection = null;
		Connection mssqlConnection = null;
		try {
			hanaConnection = DriverManager.getConnection( // 10.173.19.184
					"jdbc:sap://hxehost.grn.genre.com:30013/?autocommit=true?databaseName=SYSTEM&user=SYSTEM&password=Elster2016",
					"SYSTEM", "Elster2016");

			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");

			mssqlConnection = DriverManager.getConnection(
					"jdbc:sqlserver://USTRNTQD326.grn.genre.com:1433;databaseName=MasterData;user=SAPPI;password=SAPPI1234");

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
						"INSERT INTO EOC.MD_O_BusinessPartner "
						+ "(BusinessPartner, DeleteFlag, CreateDate, CreateUser, ModifyDate,"
					    +  "ModifyUser,	DataSourceRef, BusinessPartnerDesc, SearchTerm1, SearchTerm2,"
						+  "PartnerType, AuthorizationGroup,Language, LanguageISO, DataOrigin,"
					    +  "ToBeArchived, CentralBlock, FormOfAddress, ContactPermission, ExternalPartner,"
						+  "LetterSalutation,NotReleased, CommunicationType, PrintMode, LegalForm,"
					    + "IndustrySector, FoundationDate, LiquidationDate, InternationalLocationNumber, LegalEntity,"
						+  "DomicileState, Area, IsFETExempt, BusinessPartnerStatus, NaturalPerson,"
					    + "ImmediateParent, UltimateParent, SAPCreateUser, SAPModifyUser)"
				      + "VALUES (?,?,?,?,?,?,?,?,?,?,"
					             + "?,?,?,?,?,?,?,?,?,?,"
				                 + "?,?,?,?,?,?,?,?,?,?,"
					             + "?,?,?,?,?,?,?,?,?)"
				                 );
				

				Statement mssql_stmt = mssqlConnection.createStatement();
				ResultSet mssql_resultSet = mssql_stmt.executeQuery("select * from [Global].[MD_O_BusinessPartner]");
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
					pstmt.setString(13, mssql_resultSet.getString(13));
					pstmt.setString(14, mssql_resultSet.getString(14));
					pstmt.setString(15, mssql_resultSet.getString(15));
					pstmt.setString(16, mssql_resultSet.getString(16));
					pstmt.setString(17, mssql_resultSet.getString(17));
					pstmt.setString(18, mssql_resultSet.getString(18));
					pstmt.setString(19, mssql_resultSet.getString(19));
															
					pstmt.setString(20, mssql_resultSet.getString(20));
					pstmt.setString(21, mssql_resultSet.getString(21));
					pstmt.setString(22, mssql_resultSet.getString(22));
					pstmt.setString(23, mssql_resultSet.getString(23));
					
					pstmt.setString(24, mssql_resultSet.getString(24));
					pstmt.setString(25, mssql_resultSet.getString(25));
					pstmt.setString(26, mssql_resultSet.getString(26));
					pstmt.setString(27, mssql_resultSet.getString(27));
					pstmt.setString(28, mssql_resultSet.getString(28));
					pstmt.setString(29, mssql_resultSet.getString(29));
					pstmt.setString(30, mssql_resultSet.getString(30));
					pstmt.setString(31, mssql_resultSet.getString(31));
					pstmt.setString(32, mssql_resultSet.getString(32));
					pstmt.setString(33, mssql_resultSet.getString(33));
					pstmt.setString(34, mssql_resultSet.getString(34));
					pstmt.setString(35, mssql_resultSet.getString(35));
					pstmt.setString(36, mssql_resultSet.getString(36));
					pstmt.setString(37, mssql_resultSet.getString(37));
					pstmt.setString(38, mssql_resultSet.getString(38));
					pstmt.setString(39, mssql_resultSet.getString(39));
					
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

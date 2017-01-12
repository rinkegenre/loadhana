package com.genre.hana.load;

import java.sql.*;

public class Load_QV_FinancialsOriginalIntl {
	public static void main(String[] argv) {
		Connection hanaConnection = null;
		Connection mssqlConnection = null;
		try {
			hanaConnection = DriverManager.getConnection( // 10.173.19.184
					"jdbc:sap://hxehost.grn.genre.com:30013/?autocommit=true?databaseName=SYSTEM&user=SYSTEM&password=Elster2016",
					"SYSTEM", "Elster2016");

			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");

			mssqlConnection = DriverManager.getConnection(
	//				"jdbc:sqlserver://USTRNTQD326.grn.genre.com:1433;databaseName=EnterpriseReporting;user=SAPPI;password=SAPPI1234");
					"jdbc:sqlserver://USTRNTQD326.grn.genre.com:1433;databaseName=EnterpriseReporting;"
					+ "Integratedsecurity=true;UseNTLMv2=true;Domain=GRN;Trusted_Connection=yes;Initial Catalog=EnterpriseReporting");
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
						"INSERT INTO eoc.QV_FINANCIALSORIGINALINTL"
+ "(CreateDate,ModifyDate,AccountSK,AccountPostingNumber,CompanySK,"
+ "CedentSK,TreatySK,TreatyPeriodSK,TreatySectionSK,TreatyInvolvementSK,"
+ "AccountingUnitSK,LineOfBusinessSK,LossSK,ActivitySK,ClaimObjectSK ,"
+ "EstimateSK,EntryCodeSK,PostingRecordSK,UnderwritingAreaSK,TreatyHistorySK,"
+ "TreatySectionHistorySK,TreatyInvolvementHistorySK,BrokerSK,IBNRReserveCellSK,MCOBSK,"
+ "LineOfBusinessCode,AccountStatusCode,AccountPeriodEndDate,UnderwritingDate,OccurrenceDate,"
+ "FIPostingDate,FinancialPeriod,AccountingPeriod,AccountingYear,UnderwritingYear,"
+ "GenReUnderwritingYear,OccurrenceYear,OriginalCurrencySK , OriginalCurrency,  OriginalAmount"
						+ ")"
				      + "VALUES (?,?,?,?,?,?,?,?,?,?,"
						+ "?,?,?,?,?,?,?,?,?,?,"
						+ "?,?,?,?,?,?,?,?,?,?,"
						+ "?,?,?,?,?,?,?,?,?,?)"
				      );
				pstmt.setPoolable(true);

				Statement mssql_stmt = mssqlConnection.createStatement();
				
				ResultSet mssql_resultSet = mssql_stmt.executeQuery(
						"select "
						+ "CreateDate,ModifyDate,AccountSK,AccountPostingNumber,CompanySK,"
						+ "CedentSK,TreatySK,TreatyPeriodSK,TreatySectionSK,TreatyInvolvementSK,"
						+ "AccountingUnitSK,LineOfBusinessSK,LossSK,ActivitySK,ClaimObjectSK ,"
						+ "EstimateSK,EntryCodeSK,PostingRecordSK,UnderwritingAreaSK,TreatyHistorySK,"
						+ "TreatySectionHistorySK,TreatyInvolvementHistorySK,BrokerSK,IBNRReserveCellSK,MCOBSK,"
						+ "LineOfBusinessCode,AccountStatusCode,AccountPeriodEndDate,UnderwritingDate,OccurrenceDate,"
						+ "FIPostingDate,FinancialPeriod,AccountingPeriod,AccountingYear,UnderwritingYear,"
						+ "GenReUnderwritingYear,OccurrenceYear,OriginalCurrencySK , OriginalCurrency,  OriginalAmount"								
						+ " from [QV].[FinancialsOriginalIntl] where AccountingYear = 2015");
				long rowno = 0;
				int batchno = 1;
				while (mssql_resultSet.next()) {

					rowno++;

					pstmt.setTimestamp(1, mssql_resultSet.getTimestamp(1));
					pstmt.setTimestamp(2, mssql_resultSet.getTimestamp(2));
					pstmt.setInt(3, mssql_resultSet.getInt(3));
					pstmt.setString(4, mssql_resultSet.getString(1));
					pstmt.setInt(5, mssql_resultSet.getInt(5));
					pstmt.setInt(6, mssql_resultSet.getInt(6));					
					pstmt.setInt(7, mssql_resultSet.getInt(7));					
					pstmt.setInt(8, mssql_resultSet.getInt(8));						
					pstmt.setInt(9, mssql_resultSet.getInt(9));
					pstmt.setInt(10, mssql_resultSet.getInt(10));					
					pstmt.setInt(11, mssql_resultSet.getInt(11));					
					pstmt.setInt(12, mssql_resultSet.getInt(12));
					pstmt.setInt(13, mssql_resultSet.getInt(13));
					pstmt.setInt(14, mssql_resultSet.getInt(14));
					pstmt.setInt(15, mssql_resultSet.getInt(15));
					pstmt.setInt(16, mssql_resultSet.getInt(16));
					pstmt.setInt(17, mssql_resultSet.getInt(17));
					pstmt.setInt(18, mssql_resultSet.getInt(18));
					pstmt.setInt(19, mssql_resultSet.getInt(19));															
					pstmt.setInt(20, mssql_resultSet.getInt(20));
					pstmt.setInt(21, mssql_resultSet.getInt(21));
					pstmt.setInt(22, mssql_resultSet.getInt(22));					
					pstmt.setInt(23, mssql_resultSet.getInt(23));
					pstmt.setInt(24, mssql_resultSet.getInt(24));
					pstmt.setInt(25, mssql_resultSet.getInt(25));
					pstmt.setString(26, mssql_resultSet.getString(26));
					pstmt.setString(27, mssql_resultSet.getString(27));
					pstmt.setDate(28, mssql_resultSet.getDate(28));
					pstmt.setDate(29, mssql_resultSet.getDate(29));
					pstmt.setDate(30, mssql_resultSet.getDate(30));
					pstmt.setDate(31, mssql_resultSet.getDate(31));
					pstmt.setInt(32, mssql_resultSet.getInt(32));					
					pstmt.setInt(33, mssql_resultSet.getInt(33));
					pstmt.setInt(34, mssql_resultSet.getInt(34));
					pstmt.setInt(35, mssql_resultSet.getInt(35));
					pstmt.setInt(36, mssql_resultSet.getInt(36));
					pstmt.setInt(37, mssql_resultSet.getInt(37));
					pstmt.setInt(38, mssql_resultSet.getInt(38));
					pstmt.setString(39, mssql_resultSet.getString(39));
					pstmt.setBigDecimal(40, mssql_resultSet.getBigDecimal(40));
										
					
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

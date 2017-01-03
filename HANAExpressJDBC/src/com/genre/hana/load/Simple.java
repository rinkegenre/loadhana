package com.genre.hana.load;

import java.sql.*;

public class Simple {
   public static void main(String[] argv) {
      Connection connection = null;
      try {                  
         connection = DriverManager.getConnection(
            "jdbc:sap://10.173.19.184:30013/?autocommit=false?databaseName=SYSTEM&user=SYSTEM&password=Elster2016","SYSTEM","Elster2016");                  
      } catch (SQLException e) {
    	  
         System.err.print(e.getMessage());
         return;
      }
      if (connection != null) {
         try {
            System.out.println("Connection to HANA successful!");
            Statement stmt = connection.createStatement();
            ResultSet resultSet = stmt.executeQuery("select TEXT1, NUM1 from SYSTEM.TESTTABLE1");
            resultSet.next();
            String hello = resultSet.getString(1);
            System.out.println(hello);
       } catch (SQLException e) {
          System.err.println("Query failed!");
       }
     }
   }
}

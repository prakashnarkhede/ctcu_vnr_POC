package com.hsbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class processingLogic {
	
	public void verifyIfJoTRisValid(String record) throws SQLException, Exception {
		
		if(record.startsWith("A")) {
			
		}
		if(record.startsWith("B")) {
			
			
		}
		if(record.startsWith("Z")) {
			
		}
		String jdbcUrl = String.format(
			    "jdbc:mysql://%s/%s?cloudSqlInstance=%s"
			        + "&socketFactory=com.google.cloud.sql.mysql.SocketFactory&useSSL=false",
			"35.222.70.99",
			"ctcuDB", "maximal-ship-242013:us-central1:ctcu-vnr-tables");

				Connection connection = DriverManager.getConnection(jdbcUrl, "root", "root");
			      String query = "SELECT * FROM ctry_cde";
			      Statement st = connection.createStatement();
			      ResultSet rs = st.executeQuery(query);
			      while (rs.next())
			      {
			        String countryCode = rs.getString("Country_Code");
			        String Individual_tin = rs.getString("I_TIN_Format");
			        String Entity_tin = rs.getString("E_TIN_Format");
			        String noTINreason = rs.getString("No_TIN_Reason");
			        
			        // print the results
			        System.out.format("%s, %s, %s, %s\n", countryCode, Individual_tin, Entity_tin, noTINreason);
			      }


	}

}

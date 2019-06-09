package com.hsbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.beam.repackaged.beam_sdks_java_core.org.apache.commons.lang3.StringUtils;

public class processingLogic {
	
	String JoTR1 = "";
	String JoTR2 = "";	
	String JoTR3 = "";	
	String JoTR4 = "";	
	String JoTR5 = "";
	String JoTR1_warning = "    ";
	String JoTR2_warning = "    ";
	String JoTR3_warning = "    ";
	String JoTR4_warning = "    ";
	String JoTR5_warning = "    ";
	
	public Boolean getCountryCodeValue(String country) throws Exception {
		String jdbcUrl = String.format(
			    "jdbc:mysql://%s/%s?cloudSqlInstance=%s"
			        + "&socketFactory=com.google.cloud.sql.mysql.SocketFactory&useSSL=false",
			"35.222.70.99",
			"ctcuDB", "maximal-ship-242013:us-central1:ctcu-vnr-tables");

				Connection connection = DriverManager.getConnection(jdbcUrl, "root", "root");
			 String query = "SELECT * FROM ctry_cde where Country_Code = '"+country+"';";
			      Statement st = connection.createStatement();
			      ResultSet rs = st.executeQuery(query);
			      while (rs.next())
			      {
			        // return true if country code found in table
			        return true;
			      }
			      //returns false if country is invalid / not present in table
				return false;

	}
	
	public StringBuilder jotrValidation(String record) throws SQLException, Exception {
		processingLogic c = new processingLogic();

		StringBuilder respData = new StringBuilder(record);
		
		if(record.startsWith("A")) {			//header processing
		}
		
		if(record.startsWith("B")) {
			JoTR1 = record.substring(167, 170);
			JoTR2 = record.substring(213, 216);
			JoTR3 = record.substring(259, 262);
			JoTR4 = record.substring(305, 308);
			JoTR5 = record.substring(351, 354);
			System.out.println(JoTR1);
			System.out.println(JoTR2);
			System.out.println(JoTR3);
			System.out.println(JoTR4);
			System.out.println(JoTR5);

			if(JoTR1 != "    ") {
				if(!c.getCountryCodeValue(JoTR1)) {
					JoTR1_warning = "V202";
					respData.replace(170, 173, JoTR1_warning);
					JoTR1_warning = "    ";
				}
			} else {
				JoTR1_warning = "V201";
				respData.replace(170, 173, JoTR1_warning);
			}
			
			if(JoTR2 != "    ") {
				if(!c.getCountryCodeValue(JoTR2)) {
					JoTR2_warning = "V202";
					respData.replace(217, 220, JoTR2_warning);
					JoTR2_warning = "    ";
				}
			} else {
				JoTR2_warning = "V201";
				respData.replace(217, 220, JoTR2_warning);
			}
			if(StringUtils.isNotBlank(JoTR3.trim())) {
				if(!c.getCountryCodeValue(JoTR3)) {
					JoTR3_warning = "V202";
					respData.replace(264, 267, JoTR3_warning);
					JoTR3_warning = "    ";
				}
			} else {
				JoTR3_warning = "V201";
				respData.replace(264, 267, JoTR3_warning);
			}

			
		}
		if(record.startsWith("Z")) {
			
		}
		return respData;	
		

	}

}

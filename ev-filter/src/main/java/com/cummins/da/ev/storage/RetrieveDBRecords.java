package com.cummins.da.ev.storage;

import com.cummins.ctp.db.dao.AbstractDBDAO;
import com.cummins.ctp.db.dao.MySqlDAO;
import com.cummins.ctp.db.exceptions.DataBaseException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.cummins.da.ev.constants.Constants;



public class RetrieveDBRecords {
	private String dbReadUrl;
	  private String dbUser;
	  private String dbPassword;
	  private String dbName;
	  private String expiredMinutes;
	  private static AbstractDBDAO mySqlDao = null;
	  private static Connection connection = null;
	  
	  public RetrieveDBRecords(Map<String, String> dbConnectionVars) {
		    this.dbReadUrl = dbConnectionVars.get("dbReadUrl");
		    this.dbUser = dbConnectionVars.get("dbUser");
		    this.dbPassword = dbConnectionVars.get("dbPassword");
		    this.dbName = dbConnectionVars.get("dbName");
		    this.expiredMinutes = dbConnectionVars.get("dbResetInterval");

		  }

	  public String getSMN(String ssn, LambdaLogger logger) throws Exception {

		    if (connection == null) {
		      connection = getConnection();
		    }
		    
		  
		    PreparedStatement ps = null;
		    ResultSet rs = null;
		    String modelName = "";
		    try {
		      String query = Constants.ServiceModelNameQuery;
		      ps = connection.prepareStatement(query);
		      ps.setString(1, ssn.trim());
		      rs = ps.executeQuery();
		      if (rs.isBeforeFirst()) {
		      while (rs.next()) {
		        modelName=rs.getObject(1).toString();
		      }
		      }
		      else
		      {
		    	  logger.log("SSN "+ssn+ " NOT FOUND");
		      }
		    
		      rs.close();
		     
		    } catch (Exception e) {
		      e.printStackTrace();
		    }finally {
		          try {
			            rs.close();
			          } catch (SQLException e) {
			            e.printStackTrace();
			            throw e;

			          }
			          try {
			            ps.close();
			          } catch (SQLException e) {
			            e.printStackTrace();
			            throw e;
			          }
			        }
		    return modelName;
		  }

		  private Connection getConnection() {
			    
			    if (connection == null) {
			      if (mySqlDao == null) {
			    	
			        mySqlDao = MySqlDAO.getInstance(dbUser, dbPassword, dbReadUrl, expiredMinutes, dbName);
			        
			        try {
			        	
			          connection = (Connection) mySqlDao.getConnection();
			        
			        } catch (DataBaseException e) {
			          e.printStackTrace();
			        }
			      }
			    }
			    return connection;
			  }
	
	
}
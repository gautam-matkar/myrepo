package com.cummins.da.ev.lambdahandler;

import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.cummins.da.ev.storage.RetrieveDBRecords;

public class LambdaFunctionHandler implements RequestHandler<SNSEvent, String> {

	private RetrieveDBRecords dbretreiver = null;
	private Map<String, String> variables = new HashMap<String, String>();
	private AmazonSNS snsClient = null;
	private LambdaLogger logger;
	private String serviceModelName=null;
	  
	public void setSNS(AmazonSNS sns) {
		/*test commit*/
	    this.snsClient = sns;
	  }
	  public void setRetrieveDBRecords(RetrieveDBRecords dbretreiver) {
		    this.dbretreiver = dbretreiver;
		  }
	/*  public void setServiceModelName(String serviceModelName) {
		    this.serviceModelName = serviceModelName;
		  }
	  */
    @Override
    public String handleRequest(SNSEvent event, Context context) {
    	
    	this.logger = context.getLogger(); 
    	
        
    	if (this.variables.isEmpty()) {
  	      this.variables.putAll(getVariables());
  	    }
        
        if (this.snsClient == null) {
            this.snsClient = AmazonSNSClientBuilder.defaultClient();
          }
        
        String message = event.getRecords().get(0).getSNS().getMessage();
        this.logger.log("From SNS: " + message);
        
        /*
         * Assumptions: Incoming message is Streams SDK JSON message
						ESN & Message_Type come with quotes in SDK message
         */
        
        String messageType=null;
        String systemSerialNumber= null;
        
        
        
        JsonReader reader = Json.createReader(new StringReader(message));
        JsonObject jsonObject = reader.readObject();
       

        
        //check if EV & HB
       if(jsonObject.containsKey("sdk_message"))
       {
    	   JsonObject jsonSDKObject = jsonObject.getJsonObject("sdk_message");
    	   
    	  if(jsonSDKObject.containsKey("Engine_Serial_Number") && jsonSDKObject.containsKey("Message_Type"))
        {
    		  
    		systemSerialNumber=jsonSDKObject.get("Engine_Serial_Number").toString().replace("\"", "");
/*       	 	//////////////////////////////////////////////////////////////////////////////    
    	    //initialize
    	
	          if (dbretreiver == null) {
	    	          this.dbretreiver = new RetrieveDBRecords(this.variables);
	    	        }
	    	//DB call to get ServiceModelName 
	          
	          try {
	  			serviceModelName=this.dbretreiver.getSMN(systemSerialNumber,this.logger);
	  			} catch (Exception e) {
	  			// TODO Auto-generated catch block
	  				 this.logger.log("Error in DB Retrieval");
	  				e.printStackTrace();
	  			}
	        //check for isEV
	           
	          try {
				if(serviceModelName!=null && this.isEV(serviceModelName)){
									  	
					messageType = jsonSDKObject.get("Message_Type").toString().replace("\"","");
				  	//process only HB messages
				  	if(messageType.equalsIgnoreCase("HB"))
				  	{
				  		 this.logger.log("Received HB Message Type... SSN = " + systemSerialNumber);
				  	//write to SNS
				  		if (message != null) {
				  	      postSNS(message);
				  	    }	
				  	}
				  }
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
*/  	
    		
    		String esnList= variables.get("esnList");
    		
    		try {
				if(esnList.contains(systemSerialNumber)){
									  	
					messageType = jsonSDKObject.get("Message_Type").toString().replace("\"","");
					//process only HB messages
				  	if(messageType.equalsIgnoreCase("HB"))
				  	{
				  		 this.logger.log("Received HB Message Type... SSN = " + systemSerialNumber);
				  	//write to SNS
				  		if (message != null) {
				  	      postSNS(message);
				  	    }	
				  	}
				  	else{
				  		this.logger.log("Non-HB message - Discarding message");
				  	}
				  }
				else{
					this.logger.log("Non-EV message - Discarding message");
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
    		
        }
       }
        return message;
    }
    
    private Map<String, String> getVariables() {
        Map<String, String> vars = new HashMap<String, String>();
        vars.put("dbReadUrl", System.getenv("dbReadUrl"));
        vars.put("dbName", System.getenv("dbName"));
        vars.put("dbResetInterval", System.getenv("dbResetInterval"));
        vars.put("dbUser", System.getenv("dbUser"));
        vars.put("dbPassword", System.getenv("dbPassword"));
        vars.put("SNSTopicOut", System.getenv("SNSTopicOut"));
        vars.put("esnList", System.getenv("esnList"));

        return vars;
      }
    
   /* public boolean isEV(String SMN) throws Exception {

    	if(SMN.equalsIgnoreCase("BEV CM2450 EV101B")){
    		return true;
    	}
    	else
    	{
    		return true;
    		//return false;
    	}
    }*/
    

    
    public void postSNS(String output) {
        try {
        	
          PublishRequest publishRequest = new PublishRequest(this.variables.get("SNSTopicOut"), output);
          PublishResult publishResult = this.snsClient.publish(publishRequest);
          this.logger.log("-------Push done onto SNS------");
          this.logger.log("MessageId - " + publishResult.getMessageId());
        } catch (AmazonClientException e) {
        	 this.logger.log("Error posting to SNS: " + e.getMessage());
          throw e;
        }

      }
}

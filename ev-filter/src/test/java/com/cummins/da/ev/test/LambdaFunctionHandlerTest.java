package com.cummins.da.ev.test;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

import org.junit.Before;
import org.junit.Test;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.amazonaws.services.lambda.runtime.events.SNSEvent.SNS;
import com.amazonaws.services.lambda.runtime.events.SNSEvent.SNSRecord;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.amazonaws.util.IOUtils;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import com.cummins.da.ev.storage.RetrieveDBRecords;
import com.cummins.da.ev.lambdahandler.LambdaFunctionHandler;

/**
 *
 * A simple test harness for locally invoking your Lambda function handler.
 */
public class LambdaFunctionHandlerTest {

private AmazonSNS snsClient = mock(AmazonSNS.class);
private RetrieveDBRecords dbretreiver = mock(RetrieveDBRecords.class);

private Map<String, String> variables = new HashMap<String, String>();
private LambdaLogger logger;
String esnList;

private String serviceModelName=null;
    private Context createContext() {
        TestContext ctx = new TestContext();

        // TODO: customize your context here if needed.
        ctx.setFunctionName("EVValidation");

        return ctx;
    }

   
    @Test
    public void testLambdaFunctionHandler() {
        
    	LambdaFunctionHandler handler = new LambdaFunctionHandler();
        Context context = createContext();

        InputStream stream=null;
        try {
       	  stream = TestUtils.class.getResourceAsStream("/sns-event.json");
      	 // message = generateSNSEvent(stream).getRecords().get(0).getSNS().getMessage();
         
        } catch (Exception e) {
          System.out.println("Could not load input file");
        }

        try{
        	  SNSEvent event= generateSNSEvent(stream);
        	  
              handler.setSNS(snsClient);
//              String output=handler.handleRequest(event, context);
//              System.out.println("output : "+output);
//              Assert.assertEquals("{\"other_params\":{\"Received_Date_Time\":\"2017-12-18 16:19:42.545\"},\"sdk_message\":{\"Message_Type\": \"HB\",\"Telematics_Partner_Message_ID\": \"23163744\", \"Engine_Serial_Number\": \"14026505\",\"Equipment_ID\":\"EQUIP_IMPL_910\",\"Telematics_Partner_Name\":\"Saucon\",\"VIN\":\"4V4NC9TG8CIMPL910\", \"Customer_Reference\":\"CUSTREF_IMPL_4\", \"Telematics_Box_ID\":\"BoxID_IMPL_910\"}}", output);
 
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
           	 	//////////////////////////////////////////////////////////////////////////////    
       /* 	    //initialize
    	          if (dbretreiver == null) {
    	    	          this.dbretreiver = new RetrieveDBRecords(this.variables);
    	    	        }
    	    	//DB call to get ServiceModelName 
    	          try {
    	           when(dbretreiver.getSMN("14026505", logger)).thenReturn("BEV CM2450 EV101B");	
    	           when(dbretreiver.getSMN("11111111", logger)).thenReturn("BEV CM2450 EV101B1");
    	  			serviceModelName=this.dbretreiver.getSMN(systemSerialNumber,this.logger);
    	  		//	System.out.println(serviceModelName);
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
        		
        		
        		//String esnList= variables.get("esnList");
        		String esnList="75039927;14026505";
        		
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
//            return message;
        
        }
        catch(Exception e){
        	System.out.println("Error in Lambda" );
        }
       
    }
    
    private Map<String, String> getVariables() {
        Map<String, String> vars = new HashMap<String, String>();
        vars.put("dbReadUrl", System.getenv("dbReadUrl"));
        vars.put("dbName", System.getenv("dbName"));
        vars.put("dbResetInterval", System.getenv("dbResetInterval"));
        vars.put("dbUser", System.getenv("dbUser"));
        vars.put("dbPassword", System.getenv("dbPassword"));
        vars.put("SNSTopicOut", System.getenv("SNSTopicOut"));

        return vars;
      }
    
    public boolean isEV(String SMN) throws Exception {

    	if(SMN.equalsIgnoreCase("BEV CM2450 EV101B")){
    		return true;
    	}
    	else
    	{
    		return false;
    	}
    }
    
    public void postSNS(String output) {
        try {
        	
          PublishRequest publishRequest = new PublishRequest(this.variables.get("SNSTopicOut"), output);
          PublishResult publishResult = this.snsClient.publish(publishRequest);
          this.logger.log("-------Push done onto SNS------");
          if(publishResult!=null){
          this.logger.log("MessageId - " + publishResult.getMessageId());
          }
        } catch (AmazonClientException e) {
        	 this.logger.log("Error posting to SNS: " + e.getMessage());
          throw e;
        }

      }
    
  
      
    private SNSEvent generateSNSEvent(InputStream input) {
        SNSEvent event = new SNSEvent();
        SNSRecord record = new SNSRecord();
        record.setEventSource("aws:sns");
        record.setEventVersion("1.0");

        String message = null;
        try {
          message = IOUtils.toString(input);
        } catch (IOException e) {
          e.printStackTrace();
        }

        SNS sns = new SNS();
        sns.setMessage(message);
        record.setSns(sns);

        List<SNSRecord> list = new ArrayList<SNSRecord>();
        list.add(record);
        event.setRecords(list);

        return event;
      }
    
}


 
      
    

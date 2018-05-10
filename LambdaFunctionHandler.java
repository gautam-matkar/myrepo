package com.cummins.da.ev.lambdahandler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
great point
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.TableNameOverride;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.cummins.da.ev.dynamo.vo.CustomerThresholdVO;
import com.cummins.da.ev.parser.Parser;
import com.cummins.da.ev.storage.DatabaseLoad;
import com.cummins.da.ev.storage.DynamodbLoad;
import com.cummins.da.ev.utils.FirehoseUtil;
import com.cummins.da.ev.vo.EVDataWrapperVO;

public class LambdaFunctionHandler implements RequestHandler<SNSEvent, Object> {
	private Map<String, String> variables = new HashMap<String, String>();

	private LambdaLogger logger;
	private AmazonSNS snsClient = null;
	private String Status="";
	private static DynamoDBMapper mapper;


	public void setSNS(AmazonSNS sns) {
		this.snsClient = sns;
	}

	
	
	public Object handleRequest(SNSEvent input, Context context) {
		this.logger = context.getLogger();
		hello
		if (this.variables.isEmpty()) {
			this.variables.putAll(getVariables());
		}

		if (this.snsClient == null) {
			this.snsClient = AmazonSNSClientBuilder.defaultClient();
		}

		String message = input.getRecords().get(0).getSNS().getMessage();
		

		boolean debug= Boolean.parseBoolean(this.getVariables().get("Debug"));
		String streamname=this.getVariables().get("FirehoseStreamName");
		this.logger.log("From SNS: " + message);
		String DynamoTableName=this.getVariables().get("DynamoTableNamelkp");;

		/* Object initialization for Parser/extraction */
		Parser parse = new Parser();

		/* Object initialization for loading to MYSQL */
		DatabaseLoad evload = new DatabaseLoad(this.variables);

		/* Object initialization for DynamoDb */
		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
		DynamodbLoad dynamoload = new DynamodbLoad(this.variables);
		CustomerThresholdVO itemRetrieved = new CustomerThresholdVO();
		  DynamoDBMapperConfig config = new DynamoDBMapperConfig.Builder()
			        .withTableNameOverride(new TableNameOverride(DynamoTableName)).build();

			    mapper = new DynamoDBMapper(client, config);
		

		/* Object initialization for Datawrapper */
		EVDataWrapperVO evwprocess = new EVDataWrapperVO();

		try {
			/* JSON parsing and extraction */

			evwprocess = parse.getExtractor(message, this.logger);

			/*
			 * Threshold check and enriching message using dynamodb.Involves
			 * only reads
			 */
			
			itemRetrieved = dynamoload.getCustomerThresholds(evwprocess, mapper, this.logger);
			dynamoload.getChargingStatus(evwprocess, itemRetrieved, this.logger);

			/* Update and insert operation in mysql for EV registration */
			Status=evload.performEvRegistration(evwprocess, this.logger).getstatus();
						
			if (!Status.equalsIgnoreCase("SUCCESS")) {
				
				FirehoseUtil.postBadData("ev-data-processor", Status, message,streamname,debug );
				
			}

			/*Based on design discussion Friday 04/06/2018,it is agreed to have
			 * MYSQL.
			/* Update and insert operation in dynamodb. */
			/* dynamoload.loadToDynamoDB(evwprocess, dynamoDB); */

		} catch (Exception e) {

			this.logger.log("Error encountered  Lambda" + e.getMessage());

		}

		return "Exiting sucessfully";

	}

	private Map<String, String> getVariables() {
		Map<String, String> vars = new HashMap<String, String>();
		vars.put("dbUrl", System.getenv("dbUrl"));
		vars.put("dbName", System.getenv("dbName"));
		vars.put("dbResetInterval", System.getenv("dbResetInterval"));
		vars.put("dbUser", System.getenv("dbUser"));
		vars.put("dbPassword", System.getenv("dbPassword"));
		/*vars.put("DynamoTableName", System.getenv("DynamoTableName"));
		vars.put("DynamoIndexName", System.getenv("DynamoIndexName"));*/
		vars.put("DynamoTableNamelkp", System.getenv("DynamoTableNamelkp"));
		vars.put("SNSTopicIn", System.getenv("SNSTopicIn"));
		vars.put("FirehoseStreamName", System.getenv("FirehoseStreamName"));
		vars.put("Debug", System.getenv("Debug"));
		return vars;
	}
}

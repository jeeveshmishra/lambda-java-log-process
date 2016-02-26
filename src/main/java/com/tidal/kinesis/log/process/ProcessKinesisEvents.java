package com.tidal.kinesis.log.process;

import java.util.List;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.KinesisEventRecord;
import com.tidal.kinesis.log.process.exception.InvalidEventRecordsException;
import com.tidal.kinesis.log.process.exception.InvalidInputException;;

public class ProcessKinesisEvents implements RequestHandler<KinesisEvent, Object> {

    @Override
    public Object handleRequest(KinesisEvent input, Context context) {
    	LambdaLogger  logger = context.getLogger();
        try {
			validateInput(input);
			logger.log("Input: " + input);
			 for(KinesisEventRecord rec : input.getRecords()) {
				 logger.log(new String(rec.getKinesis().getData().array()));
		     }
			
		} catch (InvalidEventRecordsException | InvalidInputException e) {
			// TODO Auto-generated catch block
			logger.log(e.getMessage());
			e.printStackTrace();
		}
        
        // TODO: implement your handler
        return null;
    }


	/**
	 * @param input
	 * @throws InvalidEventRecordsException 
	 * @throws InvalidInputException 
	 */
	private void validateInput(KinesisEvent input) throws InvalidEventRecordsException, InvalidInputException {
		if(input != null){
        	List<KinesisEventRecord> eventRecords = input.getRecords();
        	if(eventRecords == null || eventRecords.isEmpty()){
        		throw new InvalidEventRecordsException("The Event Records are null or empty");
        	} 
        } else {
        	throw new InvalidInputException("The Event Records are null or empty");
        }
	}
}

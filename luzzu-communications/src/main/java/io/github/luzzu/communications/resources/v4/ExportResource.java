package io.github.luzzu.communications.resources.v4;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.luzzu.communications.requests.RequestBoard;
import io.github.luzzu.communications.requests.RequestValidator;
import io.github.luzzu.communications.utils.APIExceptionJSONBuilder;
import io.github.luzzu.communications.utils.APIResponse;
import io.github.luzzu.operations.lowlevel.ExceptionOutput;

@Path("/v4/")
public class ExportResource {
	
	final static Logger logger = LoggerFactory.getLogger(ExportResource.class);
	private String TRIG = MediaType.TEXT_PLAIN;
	private String JSON = MediaType.APPLICATION_JSON;


	@POST
	@Path("export/toDQV/")
	@Produces("application/trig")
	public Response exportToDQV(MultivaluedMap<String, String> formParams){			
		logger.info("Requesting assessment dates for dataset");
		
		String trigResponse = "";
		String datasetPLD = "";
		
		try {
			boolean validRequest = RequestValidator.exportValidator(formParams);
			if (validRequest) {
				datasetPLD = formParams.get("Dataset-PLD").get(0);
				logger.info("Requesting lastest quality values for dataset: {}", datasetPLD);
				
				trigResponse = RequestBoard.getMetadataInDQV(datasetPLD);
			}
		} catch (IllegalArgumentException e) {
			ExceptionOutput.output(e, "Request Cancelled",  logger);
			trigResponse = new APIExceptionJSONBuilder(datasetPLD, e).toString();
			return APIResponse.ok(trigResponse, this.JSON);
		}
		
		return APIResponse.ok(trigResponse, this.TRIG);
	}
	
	@POST
	@Path("export/toDAQ/")
	@Produces("application/trig")
	public Response exportToDAQ(MultivaluedMap<String, String> formParams){			
		logger.info("Requesting assessment dates for dataset");
		
		String trigResponse = "";
		String datasetPLD = "";
		
		try {
			boolean validRequest = RequestValidator.exportValidator(formParams);
			if (validRequest) {
				datasetPLD = formParams.get("Dataset-PLD").get(0);
				logger.info("Requesting lastest quality values for dataset: {}", datasetPLD);
				
				trigResponse = RequestBoard.getMetadataInDAQ(datasetPLD);
			}
		} catch (IllegalArgumentException e) {
			ExceptionOutput.output(e, "Request Cancelled",  logger);
			trigResponse = new APIExceptionJSONBuilder(datasetPLD, e).toString();
			return APIResponse.ok(trigResponse, this.JSON);
		}
		
		return APIResponse.ok(trigResponse, this.TRIG);
	}
}

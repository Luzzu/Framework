package io.github.luzzu.communications.resources.v4;

import java.util.HashSet;
import java.util.Set;

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
public class DatasetResource {

	final static Logger logger = LoggerFactory.getLogger(DatasetResource.class);
	private String JSON = MediaType.APPLICATION_JSON;
	
	@POST
	@Path("metric/observation/profile")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getObservationProfile(MultivaluedMap<String, String> formParams){
		String jsonResponse = "";
		String datasetPLD = "";
		String observation = "";
		
		try {
			boolean validRequest = RequestValidator.observationProfile(formParams);
			if (validRequest) {
				datasetPLD = formParams.get("Dataset-PLD").get(0);
				observation = formParams.get("Observation").get(0);
				logger.info("Requested Profile for Observation: {}", observation);
				
				jsonResponse = RequestBoard.getProfilingForObservation(observation, datasetPLD);
			}
		} catch (IllegalArgumentException e) {
			ExceptionOutput.output(e, "Request Cancelled",  logger);
			jsonResponse = new APIExceptionJSONBuilder(observation, e).toString();
		}
		
		return APIResponse.ok(jsonResponse.toString(), this.JSON);
	}
	
	@POST
	@Path("dataset/latest/")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getDatasetQualityResults(MultivaluedMap<String, String> formParams){			
		logger.info("Requesting lastest quality values for dataset");
		
		String jsonResponse = "";
		String datasetPLD = "";
		
		try {
			boolean validRequest = RequestValidator.datasetQualityValues(formParams);
			if (validRequest) {
				datasetPLD = formParams.get("Dataset-PLD").get(0);
				logger.info("Requesting lastest quality values for dataset: {}", datasetPLD);
				
				jsonResponse = RequestBoard.getLatestValuesForDataset(datasetPLD);
			}
		} catch (IllegalArgumentException e) {
			ExceptionOutput.output(e, "Request Cancelled",  logger);
			jsonResponse = new APIExceptionJSONBuilder(datasetPLD, e).toString();
		}
		
		return APIResponse.ok(jsonResponse.toString(), this.JSON);
	}
	
	@POST
	@Path("dataset/assessment-dates/")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getDatasetAssessmentDates(MultivaluedMap<String, String> formParams){			
		logger.info("Requesting assessment dates for dataset");
		
		String jsonResponse = "";
		String datasetPLD = "";
		
		try {
			boolean validRequest = RequestValidator.datasetQualityValues(formParams);
			if (validRequest) {
				datasetPLD = formParams.get("Dataset-PLD").get(0);
				logger.info("Requesting lastest quality values for dataset: {}", datasetPLD);
				
				jsonResponse = RequestBoard.getAssessmentDatesForDataset(datasetPLD);
			}
		} catch (IllegalArgumentException e) {
			ExceptionOutput.output(e, "Request Cancelled",  logger);
			jsonResponse = new APIExceptionJSONBuilder(datasetPLD, e).toString();
		}
		
		return APIResponse.ok(jsonResponse.toString(), this.JSON);
	}
	
	@POST
	@Path("dataset/quality/")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getDatasetObservationForDate(MultivaluedMap<String, String> formParams){			
		logger.info("Requesting quality values for dataset at particular date");
		
		String jsonResponse = "";
		String datasetPLD = "";
		String date = "";

		try {
			boolean validRequest = RequestValidator.datasetObservationForDate(formParams);
			if (validRequest) {
				datasetPLD = formParams.get("Dataset-PLD").get(0);
				date = formParams.get("Date").get(0);
				logger.info("Requesting quality values for dataset: {} on date : {}", datasetPLD, date);
				
				jsonResponse = RequestBoard.getValuesForDatasetWithDate(datasetPLD, date);
			}
		} catch (IllegalArgumentException e) {
			ExceptionOutput.output(e, "Request Cancelled",  logger);
			jsonResponse = new APIExceptionJSONBuilder(datasetPLD, e).toString();
		}
		
		return APIResponse.ok(jsonResponse.toString(), this.JSON);
	}
	
	@POST
	@Path("compare/dataset-metrics/")
	//previously 	@Path("post/visualisation/dsvsm")
	@Produces(MediaType.APPLICATION_JSON)
	public Response compareDatasetsAgainstOneMetric(MultivaluedMap<String, String> formParams){
		logger.info("Requesting to compare multiple datasets against a metric");
		
		String jsonResponse = "";
		Set<String> datasetPLDs = null;
		String metric = "";
		
		try {
			boolean validRequest = RequestValidator.datasetsVsMetricValidator(formParams);
			if (validRequest) {
				datasetPLDs = new HashSet<String>(formParams.get("Dataset-PLD"));
				metric = formParams.get("Metric").get(0);
				jsonResponse = RequestBoard.compareDatasetsOnMetric(datasetPLDs, metric);
			}
		} catch (IllegalArgumentException e) {
			ExceptionOutput.output(e, "Request Cancelled",  logger);
			jsonResponse = new APIExceptionJSONBuilder("", e).toString();
		}

		
		return APIResponse.ok(jsonResponse.toString(), this.JSON);
	}
}

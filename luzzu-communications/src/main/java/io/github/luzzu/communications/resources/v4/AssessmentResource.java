package io.github.luzzu.communications.resources.v4;

import java.io.StringReader;
import java.util.concurrent.ExecutionException;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;

import io.github.luzzu.communications.ExtendedCallable;
import io.github.luzzu.communications.exceptions.ResourceNotFoundException;
import io.github.luzzu.communications.requests.AssessmentStatus;
import io.github.luzzu.communications.requests.RequestBoard;
import io.github.luzzu.communications.requests.RequestValidator;
import io.github.luzzu.communications.utils.APIExceptionJSONBuilder;
import io.github.luzzu.communications.utils.APIResponse;
import io.github.luzzu.exceptions.LuzzuIOException;
import io.github.luzzu.exceptions.MetricProcessingException;
import io.github.luzzu.exceptions.ProcessorNotEnabled;
import io.github.luzzu.exceptions.SyntaxErrorException;
import io.github.luzzu.io.ProcessorController;
import io.github.luzzu.operations.lowlevel.ExceptionOutput;

/**
 * REST resource, providing the functionalities to assess the quality of datasets, 
 * with respect to a set of metrics of interest
 * 
 * @author Jeremy Debattista
 * @version 4.0.0
 * 
 */

@Path("/v4/")
public class AssessmentResource {
	
	final static Logger logger = LoggerFactory.getLogger(AssessmentResource.class);
	private String JSON = MediaType.APPLICATION_JSON;
	
	
	// --- GET REQUESTS --- //
	@GET
	@Path("assessment/statistics/{request-id}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getStatisticsForRequest(@PathParam("request-id") String requestID){
		logger.info("Requested Statistics for Request: {}", requestID);
		String jsonResponse = "";
		
		try {
			jsonResponse = RequestBoard.getRequestStatistics(requestID);
		} catch (ResourceNotFoundException | LuzzuIOException e )  {
			ExceptionOutput.output(e, "Assessment Status Request",  logger);
			jsonResponse = new APIExceptionJSONBuilder(requestID, e).toString();
		} 
		
		return APIResponse.ok(jsonResponse.toString(), this.JSON);
	}
	
	@GET
	@Path("assessment/status/{request-id}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response status(@PathParam("request-id") String requestID){
		logger.info("Requested Status for Request: {}", requestID);
		String jsonResponse = "";
		
		try {
			jsonResponse = RequestBoard.getRequestStatus(requestID);
		} catch (InterruptedException | ExecutionException | ResourceNotFoundException e )  {
			ExceptionOutput.output(e, "Assessment Status Request",  logger);
			jsonResponse = new APIExceptionJSONBuilder(requestID, e).toString();
		} 
		
		return APIResponse.ok(jsonResponse.toString(), this.JSON);
	}

	@GET
	@Path("assessment/pending")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getPendingRequests(){
		logger.info("Requested list of Pending Requests");
		return APIResponse.ok(RequestBoard.getAllRequests(AssessmentStatus.INPROGRESS).toString(), this.JSON);
	}
	
	@GET
	@Path("assessment/successful")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getSuccessfulRequests(){
		logger.info("Requested list of Successful Requests");
		return APIResponse.ok(RequestBoard.getAllRequests(AssessmentStatus.SUCCESSFUL).toString(), this.JSON);
	}
	
	@GET
	@Path("assessment/failed")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getFailedRequests(){
		logger.info("Requested list of Failed Requests");
		return APIResponse.ok(RequestBoard.getAllRequests(AssessmentStatus.FAILED).toString(), this.JSON);
	}
	
	@GET
	@Path("assessment/cancelled")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getCancelledRequests(){
		logger.info("Requested list of Failed Requests");
		return APIResponse.ok(RequestBoard.getAllRequests(AssessmentStatus.CANCELLED).toString(), this.JSON);
	}
	
	// --- POST REQUESTS --- //
	/**
	 * @param Request-ID
	 */
	@POST
	@Path("assessment/cancel")
	@Produces(MediaType.APPLICATION_JSON)
	public Response cancelRequest(MultivaluedMap<String, String> formParams){
		String requestID = "No Request-ID Provided";
		String jsonResponse = "";
		
		try {
			boolean validRequest = RequestValidator.cancelRequest(formParams);
			if (validRequest) {
				requestID = formParams.get("Request-ID").get(0);
				logger.info("Requested the cancellation of Request: {}", requestID);
				System.out.printf("Requested the cancellation of Request: %s", requestID);
				jsonResponse = RequestBoard.cancelRequest(requestID);
			}
		} catch (IllegalArgumentException | ResourceNotFoundException | LuzzuIOException e) {
			ExceptionOutput.output(e, "Assessment Cancellation Request",  logger);
			jsonResponse = new APIExceptionJSONBuilder(requestID, e).toString();
		}
		
		return APIResponse.ok(jsonResponse.toString(), this.JSON);
	}
	
	
	/**
	 * 
	 * @param Dataset-Location
	 * @param Metrics-Configuration
	 * @param Dataset-PLD
	 * @param Is-Sparql-Endpoint
	 * @param Quality-Report-Required
	 * 
	 * @param (Optional) Crawl-Date
	 */
	@POST
	@Path("assessment/compute")
	@Produces(MediaType.APPLICATION_JSON)
	public Response computeQuality(MultivaluedMap<String, String> parameters) {
		String jsonResponse = "";
		try {
			boolean validRequest = RequestValidator.computeRequest(parameters);
			if (validRequest) {
				String requestID = requestQualityAssessment(parameters);
				System.out.println("Added request: "+ requestID);
				logger.info("Request ID: {}", requestID);
				jsonResponse = RequestBoard.getRequestStatus(requestID);
			}
		} catch (IllegalArgumentException | InterruptedException | ExecutionException | ResourceNotFoundException e) {
			ExceptionOutput.output(e, "Assessment Computation Request",  logger);
			jsonResponse = new APIExceptionJSONBuilder(null, e).toString();
		}
		return APIResponse.ok(jsonResponse.toString(), this.JSON);
	}
	
	private String requestQualityAssessment(MultivaluedMap<String, String> parameters) {
		final String datasetURI = parameters.get("Dataset-Location").get(0);
		final String  jsonStrMetricsConfig = parameters.get("Metrics-Configuration").get(0);
		final String  baseURI = parameters.get("Dataset-PLD").get(0);
		final Boolean isSparql = Boolean.parseBoolean(parameters.get("Is-Sparql-Endpoint").get(0));
		final Boolean genQualityReport = Boolean.parseBoolean(parameters.get("Quality-Report-Required").get(0));
		
		logger.info("Assessment Request -\nDataset URI: {}\nBase URI: {}\nMetrics: {}\nIs-Sparql: {}\nGenerate Problem Report: {}",
				datasetURI, baseURI, jsonStrMetricsConfig, isSparql, genQualityReport);

		final String crawlDate;
		if (parameters.containsKey("Crawl-Date")) {
			crawlDate = parameters.get("Crawl-Date").get(0);
			logger.debug("Processing request parameters. Dataset-Location: {}; Quality-Report-Required: {}; Metrics-Configuration: {}; Dataset-PLD: {}; Is-Sparql-Endpoint: {}; Crawl-Date: {}", 
					datasetURI, genQualityReport, jsonStrMetricsConfig, baseURI, isSparql, crawlDate);
		} else {
			crawlDate = null;
			logger.debug("Processing request parameters. Dataset-Location: {}; Quality-Report-Required: {}; Metrics-Configuration: {}; Dataset-PLD: {}; Is-Sparql-Endpoint: {}", 
					datasetURI, genQualityReport, jsonStrMetricsConfig, baseURI, isSparql);
//			System.out.printf("Processing request parameters. Dataset-Location: %s; Quality-Report-Required: %s; Metrics-Configuration: %s; Dataset-PLD: %s; Is-Sparql-Endpoint: %s", 
//					datasetURI, genQualityReport, jsonStrMetricsConfig, baseURI, isSparql);				

		}
		
		Model _modelConfig = ModelFactory.createDefaultModel();
		RDFDataMgr.read(_modelConfig, new StringReader(jsonStrMetricsConfig), null, Lang.JSONLD);
		final Model modelConfig = _modelConfig;
		
		ExtendedCallable<Boolean> newRequest = new ExtendedCallable<Boolean>(){
			public Boolean call()  throws LuzzuIOException, InterruptedException, MetricProcessingException {
				try {
					strmProc = ProcessorController.getInstance().decide(baseURI, datasetURI, genQualityReport, modelConfig, isSparql, crawlDate);
					strmProc.processorWorkFlow();
				} catch (SyntaxErrorException e) {
					// Do nothing because this exception is already logged
					strmProc.cleanUp();
					logger.info("Quality computation halted due to syntax error. Dataset-PLD: {}", baseURI);
					System.out.printf("\nQuality computation halted due to syntax error. Dataset-PLD: %s", baseURI);
					return new Boolean(false);						
				} catch (ProcessorNotEnabled notEnabled) {
					logger.info("Quality computation could not be started as processor is not enabled. Dataset-PLD: {}", baseURI);
					System.out.printf("\nQuality computation could not be started as processor is not enabled. Dataset-PLD: %s", baseURI);
					ExceptionOutput.output(notEnabled, "Assessment Cancelled",  logger);
					return new Boolean(false);	
				} catch (LuzzuIOException | InterruptedException otherexp) {
					return new Boolean(false);
				}
				strmProc.cleanUp();
				logger.info("Quality computation request completed. Dataset-PLD: {}", baseURI);
				System.out.printf("\nQuality computation request completed. Dataset-PLD: %s", baseURI);
				
				return new Boolean(true);						
			}
		};
		
		String requestID = RequestBoard.addRequest(newRequest, datasetURI, baseURI, isSparql);
		return requestID;
	}
}

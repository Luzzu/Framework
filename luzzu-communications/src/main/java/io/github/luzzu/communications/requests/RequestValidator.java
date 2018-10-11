package io.github.luzzu.communications.requests;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.ws.rs.core.MultivaluedMap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class RequestValidator {

	public static boolean cancelRequest(MultivaluedMap<String, String> parameters) {
		final List<String> requestIDparam = parameters.get("Request-ID");
		
		if(requestIDparam == null || requestIDparam.size() <= 0) {
			throw new IllegalArgumentException("Request-ID parameter was not provided");
		} else
			return true;
	}
	
	public static boolean computeRequest(MultivaluedMap<String, String> parameters) {
		
		final List<String> lstDatasetURI = parameters.get("Dataset-Location");
		final List<String> lstQualityReportReq = parameters.get("Quality-Report-Required");
		final List<String> lstMetricsConfig = parameters.get("Metrics-Configuration");
		final List<String> lstBaseUri = parameters.get("Dataset-PLD");
		final List<String> lstIsSparql = parameters.get("Is-Sparql-Endpoint");
											
		if(lstDatasetURI == null || lstDatasetURI.size() <= 0) {
			throw new IllegalArgumentException("Dataset-Location parameter was not provided");
		}
		if(lstQualityReportReq == null || lstQualityReportReq.size() <= 0) {
			throw new IllegalArgumentException("Quality-Report-Required parameter was not provided");
		}
		if(lstMetricsConfig == null || lstMetricsConfig.size() <= 0) {
			throw new IllegalArgumentException("Metrics-Configuration parameter was not provided");
		}
		if(lstBaseUri == null || lstBaseUri.size() <= 0) {
			throw new IllegalArgumentException("Dataset-PLD parameter was not provided");
		}
		if(lstIsSparql == null || lstIsSparql.size() <= 0) {
			throw new IllegalArgumentException("Is-Sparql-Endpoint parameter was not provided");
		}
		
		return true;
	}
	
	public static boolean observationProfile(MultivaluedMap<String, String> parameters) {
		final List<String> lstDatasetPLD = parameters.get("Dataset-PLD");
		final List<String> lstObservation = parameters.get("Observation");
		
		if(lstDatasetPLD == null || lstDatasetPLD.size() <= 0) {
			throw new IllegalArgumentException("Dataset-PLD parameter was not provided");
		}
		if(lstObservation == null || lstObservation.size() <= 0) {
			throw new IllegalArgumentException("Observation parameter was not provided");
		}
		
		if(lstDatasetPLD.size() > 1) {
			throw new IllegalArgumentException("Dataset-PLD parameter requires only one value");
		}
		if(lstObservation.size() > 1) {
			throw new IllegalArgumentException("Observation parameter requires only one value");
		}
		
		return true;
	}
	
	public static boolean datasetQualityValues(MultivaluedMap<String, String> parameters) {
		final List<String> lstDatasetPLD = parameters.get("Dataset-PLD");
		
		if(lstDatasetPLD == null || lstDatasetPLD.size() <= 0) {
			throw new IllegalArgumentException("Dataset-PLD parameter was not provided");
		}
		
		if(lstDatasetPLD.size() > 1) {
			throw new IllegalArgumentException("Dataset-PLD parameter requires only one value");
		}
		
		return true;
	}
	
	public static boolean datasetObservationForDate(MultivaluedMap<String, String> parameters) {
		final List<String> lstDatasetPLD = parameters.get("Dataset-PLD");
		final List<String> lstDate = parameters.get("Date");

		if(lstDatasetPLD == null || lstDatasetPLD.size() <= 0) {
			throw new IllegalArgumentException("Dataset-PLD parameter was not provided");
		}
		
		if(lstDatasetPLD.size() > 1) {
			throw new IllegalArgumentException("Dataset-PLD parameter requires only one value");
		}
		
		if(lstDate == null || lstDate.size() <= 0) {
			throw new IllegalArgumentException("Date parameter was not provided");
		}
		
		SimpleDateFormat fm = new SimpleDateFormat("yyyy-MM-dd");
		try {
			fm.parse(lstDate.get(0));
		} catch (ParseException e) {
			throw new IllegalArgumentException("Date should be in the yyyy-MM-dd format");
		}		
		
		return true;
	}
	
	public static boolean weightedRankingValidator(String json) {
		try {
			ObjectMapper mapper = new ObjectMapper();
			JsonNode rootNode = mapper.readTree(json);
			Iterator<JsonNode> iter = rootNode.elements();
			if (iter.hasNext()) {
				AtomicInteger itemNumber = new AtomicInteger(0);
				iter.forEachRemaining(config -> {
					itemNumber.incrementAndGet();
					if (!config.has("type")) throw new IllegalArgumentException("type key missing in configuration number: "+itemNumber.get());
					if (!config.has("uri")) throw new IllegalArgumentException("uri key missing in configuration number: "+itemNumber.get());
					if (!config.has("weight")) throw new IllegalArgumentException("weight key missing in configuration number: "+itemNumber.get());
				});
			} else {
				throw new IllegalArgumentException("No Ranking Configuration found in JSON");
			}
		} catch (IOException e) {
			throw new IllegalArgumentException("Incorrect JSON format");
		}
		
		return true;
	}
	
	public static boolean datasetsVsMetricValidator(MultivaluedMap<String, String> parameters) {
		final List<String> lstDatasetPLD = parameters.get("Dataset-PLD");
		final List<String> lstMetric = parameters.get("Metric");

		if(lstDatasetPLD == null || lstDatasetPLD.size() <= 0) {
			throw new IllegalArgumentException("Dataset-PLD parameter was not provided. There should be at least one or more Dataset-PLDs defined.");
		}
		
		
		if(lstMetric == null || lstMetric.size() <= 0) {
			throw new IllegalArgumentException("Metric parameter was not provided");
		}
		
		if(lstMetric.size() > 1) {
			throw new IllegalArgumentException("lstMetric parameter requires only one value");
		}
		
		return true;
	}
	
	public static boolean exportValidator(MultivaluedMap<String, String> parameters) {
		final List<String> lstDatasetPLD = parameters.get("Dataset-PLD");
		
		if(lstDatasetPLD == null || lstDatasetPLD.size() <= 0) {
			throw new IllegalArgumentException("Dataset-PLD parameter was not provided");
		}
		
		if(lstDatasetPLD.size() > 1) {
			throw new IllegalArgumentException("Dataset-PLD parameter requires only one value");
		}
		
		return true;
	}
}

package io.github.luzzu.web.jsondatatypes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.luzzu.operations.lowlevel.ExceptionOutput;



public class ComparingDatasetObject {
	
	final static Logger logger = LoggerFactory.getLogger(ComparingDatasetObject.class);
			
	private String pld = null;
	private ObservationObject observation = null;
	private String metric = null;

	
	@JsonProperty("Latest-Observation")
	public ObservationObject getObservation() {
		return observation;
	}
	
	public void setObservation(ObservationObject observation) {
		this.observation = observation;
	}
	
	@JsonProperty("Dataset-PLD")
	public String getName() {
		return pld;
	}
	
	public void setName(String pld) {
		this.pld = pld;
	}

	@JsonProperty("Metric-Observation")
	public String getMetric() {
		return metric;
	}

	public void setMetric(String metric) {
		this.metric = metric;
	}
	
	public String toString() {
		ObjectMapper mapper = new ObjectMapper();
		String json = "{}";
		
		try {
			json = mapper.writeValueAsString(this);
		} catch (JsonProcessingException e) {
			ExceptionOutput.output(e, "API Exception object serialisation",  logger);
		}
		
		return json;
	}
}

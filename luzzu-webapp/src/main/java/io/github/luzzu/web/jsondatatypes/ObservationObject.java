package io.github.luzzu.web.jsondatatypes;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.luzzu.operations.lowlevel.ExceptionOutput;
import io.github.luzzu.semantics.datatypes.Observation;

public class ObservationObject {
	
	final static Logger logger = LoggerFactory.getLogger(ObservationObject.class);
	
	private String observationURI = null;
//	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
	private String dateComputed = null;
	private Object value = null;
	private String valueType = null;
	private String computedOn = null;
	private String graphURI = null;
	
	public ObservationObject() {}
	
	public ObservationObject(Observation observation) {
		this.setObservationURI(observation.getObservationURI().toString());
		this.setDateComputed(observation.getDateComputed());
		this.setValue(observation.getValue());
		this.setValueType(observation.getValueType());
		this.setComputedOn(observation.getComputedOn().toString());
		this.setGraphURI(observation.getGraphURI().toString());
	}

	public ObservationObject(String observationURI, Date dateComputed, Object value, String valueType, String computedOn, String graphURI){
		this.setObservationURI(observationURI);
		this.setDateComputed(dateComputed);
		this.setValue(value);
		this.setValueType(valueType);
		this.setComputedOn(computedOn);
		this.setGraphURI(graphURI);
	}

	@JsonProperty("Observation-URI")
	public String getObservationURI() {
		return observationURI;
	}

	public void setObservationURI(String observationURI) {
		this.observationURI = observationURI;
	}

	@JsonProperty("Date-Computed")
	public String getDateComputed() {
		return dateComputed;
	}

	public void setDateComputed(Date dateComputed) {
		this.dateComputed = io.github.luzzu.operations.lowlevel.Date.dateToString(dateComputed);
	}

	@JsonProperty("Value")
	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	@JsonProperty("Value-Type")
	public String getValueType() {
		return valueType;
	}

	public void setValueType(String valueType) {
		this.valueType = valueType;
	}

	@JsonProperty("Computed-On")
	public String getComputedOn() {
		return computedOn;
	}

	public void setComputedOn(String computedOn) {
		this.computedOn = computedOn;
	}

	@JsonProperty("Graph-URI")
	public String getGraphURI() {
		return graphURI;
	}

	public void setGraphURI(String graphURI) {
		this.graphURI = graphURI;
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

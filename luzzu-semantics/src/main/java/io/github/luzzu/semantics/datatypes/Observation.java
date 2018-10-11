package io.github.luzzu.semantics.datatypes;

import java.util.Date;

import org.apache.jena.rdf.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;


public class Observation implements Comparable<Observation> {
	
	final static Logger logger = LoggerFactory.getLogger(Observation.class);

	private Resource observationURI = null;
	private Date dateComputed = null;
	private Object value = null;
	private String valueType = null;
	private Resource computedOn = null;
	private Resource graphURI = null;
	private Boolean isEstimate = null;
	
	public Observation() {}

	public Observation(Resource observationURI, Date dateComputed, Object value, String valueType, Resource computedOn, Resource graphURI, Boolean isEstimate){
		this.setObservationURI(observationURI);
		this.setDateComputed(dateComputed);
		this.setValue(value);
		this.setValueType(valueType);
		this.setComputedOn(computedOn);
		this.setGraphURI(graphURI);
		this.setIsEstimate(isEstimate);
	}

	public Resource getObservationURI() {
		return observationURI;
	}

	public void setObservationURI(Resource observationURI) {
		this.observationURI = observationURI;
	}

	public Date getDateComputed() {
		return dateComputed;
	}

	public void setDateComputed(Date dateComputed) {
		this.dateComputed = dateComputed;
	}

	@JsonProperty("Value")
	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	public int compareTo(Observation anotherObservation) {
		if (this.getDateComputed().after(anotherObservation.getDateComputed())) return 1;
		if (this.getDateComputed().before(anotherObservation.getDateComputed())) return -1;
		else return 0;
	}

	public String getValueType() {
		return valueType;
	}

	public void setValueType(String valueType) {
		this.valueType = valueType;
	}

	public Resource getComputedOn() {
		return computedOn;
	}

	public void setComputedOn(Resource computedOn) {
		this.computedOn = computedOn;
	}

	public Resource getGraphURI() {
		return graphURI;
	}

	public void setGraphURI(Resource graphURI) {
		this.graphURI = graphURI;
	}

	public Boolean getIsEstimate() {
		return isEstimate;
	}

	public void setIsEstimate(Boolean isEstimate) {
		this.isEstimate = isEstimate;
	}
	
}

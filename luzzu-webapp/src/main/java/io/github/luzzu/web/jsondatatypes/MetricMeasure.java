package io.github.luzzu.web.jsondatatypes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.luzzu.operations.lowlevel.ExceptionOutput;


public class MetricMeasure {

	private String label = null;
	private String comment = null;
	private String uri = null;

	final static Logger logger = LoggerFactory.getLogger(DimensionMeasure.class);

	@JsonProperty("Label")
	public String getLabel() {
		return label;
	}
	public void setLabel(String label) {
		this.label = label;
	}
	
	@JsonProperty("Comment")
	public String getComment() {
		return comment;
	}
	public void setComment(String comment) {
		this.comment = comment;
	}
	
	@JsonProperty("URI")
	public String getUri() {
		return uri;
	}
	public void setUri(String uri) {
		this.uri = uri;
	}
	
	@Override
	public boolean equals(Object other){
		if (other instanceof MetricMeasure){
			MetricMeasure _other = (MetricMeasure) other;
			return this.uri.equals(_other.uri);
		} else return false;
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

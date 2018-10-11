package io.github.luzzu.web.jsondatatypes.observations;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.luzzu.operations.lowlevel.ExceptionOutput;


@JsonInclude(Include.NON_NULL)
public class ProfilingProperty {
	final static Logger logger = LoggerFactory.getLogger(EstimationParameter.class);

	private String label;
	private String comment;
	private Object value;
	
	public ProfilingProperty(String label, String comment, Object value) {
		this.label = label;
		this.comment = comment;
		this.value = value;
	}

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
	
	@JsonProperty("Value")
	public Object getValue() {
		return value;
	}
	
	public void setValue(Object value) {
		this.value = value;
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

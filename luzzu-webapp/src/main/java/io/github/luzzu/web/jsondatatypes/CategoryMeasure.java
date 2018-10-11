package io.github.luzzu.web.jsondatatypes;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.luzzu.operations.lowlevel.ExceptionOutput;


public class CategoryMeasure {

	private String label = null;
	private String comment = null;
	private String uri = null;
	private List<DimensionMeasure> dimensions = new ArrayList<DimensionMeasure>();
	
	final static Logger logger = LoggerFactory.getLogger(CategoryMeasure.class);
	
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
	
	@JsonProperty("Dimensions")
	public List<DimensionMeasure> getDimensions() {
		return dimensions;
	}
	public void addDimension(DimensionMeasure dimension) {
		this.dimensions.add(dimension);
	} 
	
	@Override
	public boolean equals(Object other){
		if (other instanceof CategoryMeasure){
			CategoryMeasure _other = (CategoryMeasure) other;
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

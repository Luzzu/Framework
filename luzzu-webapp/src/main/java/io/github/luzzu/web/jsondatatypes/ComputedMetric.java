package io.github.luzzu.web.jsondatatypes;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.jena.rdf.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.luzzu.operations.lowlevel.ExceptionOutput;


@JsonInclude(Include.NON_NULL)
public class ComputedMetric {
	private String inDimension = null;
	private String inCategory = null;
	private String name = null;
	private String uri = null;
	@JsonIgnore private Resource metric_uri = null;

	private List<ObservationObject> observations = new ArrayList<ObservationObject>();
	
	@JsonIgnore private Set<String> commonDatasets = new HashSet<String>();

	final static Logger logger = LoggerFactory.getLogger(ComputedMetric.class);
	
	@JsonProperty("Metric-Label")
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	@JsonProperty("Metric-URI")
	public String getUri() {
		return uri;
	}
	
	public void setUri(String uri) {
		this.uri = uri;
	}
	
	public Set<String> getCommonDatasets() {
		return commonDatasets;
	}
	
	public void setCommonDatasets(Set<String> commonDatasets) {
		this.commonDatasets = commonDatasets;
	}
	
	@JsonProperty("Dimension")
	public String getInDimension() {
		return inDimension;
	}
	
	public void setInDimension(String inDimension) {
		this.inDimension = inDimension;
	}
	
	@JsonProperty("Category")
	public String getInCategory() {
		return inCategory;
	}
	
	public void setInCategory(String inCategory) {
		this.inCategory = inCategory;
	}
	
	@JsonProperty("Observations")
	public List<ObservationObject> getObservations() {
		return this.observations;
	}
	
	public void addObservations(ObservationObject observation) {
		if (this.observations == null) this.observations = new ArrayList<ObservationObject>();
		this.observations.add(observation);
	}
	
	public Resource getMetric_uri() {
		return metric_uri;
	}

	public void setMetric_uri(Resource metric_uri) {
		this.metric_uri = metric_uri;
	}
	
	public int hashCode(){
		return this.uri.hashCode();
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

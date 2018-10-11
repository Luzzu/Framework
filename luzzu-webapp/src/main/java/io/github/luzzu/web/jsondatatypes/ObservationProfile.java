package io.github.luzzu.web.jsondatatypes;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.luzzu.operations.lowlevel.ExceptionOutput;
import io.github.luzzu.web.commons.FieldType;
import io.github.luzzu.web.jsondatatypes.observations.EstimationParameter;
import io.github.luzzu.web.jsondatatypes.observations.ProfilingProperty;
import io.github.luzzu.web.jsondatatypes.observations.ProvenanceProperty;

/**
 * The class encapsulates an Observation Profile
 * 
 * @author Jeremy Debattista
 *
 */
@JsonInclude(Include.NON_NULL)
public class ObservationProfile {

	final static Logger logger = LoggerFactory.getLogger(ObservationProfile.class);
	
	private Long totalDatasetTriples = null;
	private Long totalDatasetTriplesAssessed = null;
	private String estimationTechniqueUsed = null;
	private FieldType estimationTechniqueUsedType = null;
	private List<EstimationParameter> estimationParameters = null; 
	private List<ProfilingProperty> profilingProperties = null;
	private List<ProvenanceProperty> provenanceProperties = null;
	
	public ObservationProfile() { }

	public ObservationProfile(Long totalDatasetEntities, Long totalDatasetEntitiesAssessed,
			String estimationTechniqueUsed, FieldType estimationTechniqueUsedType,
			List<EstimationParameter> estimationParameters, List<ProfilingProperty> profilingProperties,
			List<ProvenanceProperty> provenanceProperties) {
		
		this.totalDatasetTriples = totalDatasetEntities;
		this.totalDatasetTriplesAssessed = totalDatasetEntitiesAssessed;
		this.estimationTechniqueUsed = estimationTechniqueUsed;
		this.estimationTechniqueUsedType = estimationTechniqueUsedType;
		this.estimationParameters = estimationParameters;
		this.profilingProperties = profilingProperties;
		this.provenanceProperties = provenanceProperties;
		
	}

	@JsonProperty("Total-Dataset-Triples")
	public Long getTotalDatasetTriples() {
		return totalDatasetTriples;
	}
	
	public void setTotalDatasetTriples(Long totalDatasetTriples) {
		this.totalDatasetTriples = totalDatasetTriples;
	}
	
	@JsonProperty("Total-Dataset-Triples-Assessed")
	public Long getTotalDatasetTriplesAssessed() {
		return totalDatasetTriplesAssessed;
	}
	
	public void setTotalDatasetTriplesAssessed(Long totalDatasetTriplesAssessed) {
		this.totalDatasetTriplesAssessed = totalDatasetTriplesAssessed;
	}
	
	@JsonProperty("Estimation-Technique-Used")
	public String getEstimationTechniqueUsed() {
		return estimationTechniqueUsed;
	}
	
	public void setEstimationTechniqueUsed(String estimationTechniqueUsed) {
		this.estimationTechniqueUsed = estimationTechniqueUsed;
	}
	
	@JsonProperty("Estimation-Technique-Used-ValueType")
	public FieldType getEstimationTechniqueUsedType() {
		return estimationTechniqueUsedType;
	}
	
	public void setEstimationTechniqueUsedType(FieldType estimationTechniqueUsedType) {
		this.estimationTechniqueUsedType = estimationTechniqueUsedType;
	}
	
	@JsonProperty("Profiling-Properties")
	public List<ProfilingProperty> getProfilingProperties() {
		return profilingProperties;
	}
	
	public void addProfilingProperty(ProfilingProperty profilingProperty) {
		if (this.profilingProperties == null) this.profilingProperties = new ArrayList<ProfilingProperty>();
		this.profilingProperties.add(profilingProperty);
	}
	
	@JsonProperty("Provenance-Properties")
	public List<ProvenanceProperty> getProvenanceProperties() {
		return provenanceProperties;
	}
	
	public void addProvenanceProperties(ProvenanceProperty provenanceProperty) {
		if (this.provenanceProperties == null) this.provenanceProperties = new ArrayList<ProvenanceProperty>();
		this.provenanceProperties.add(provenanceProperty);
	}

	@JsonProperty("Estimation-Parameter")
	public List<EstimationParameter> getEstimationParameters() {
		return estimationParameters;
	}

	public void addEstimationParameters(EstimationParameter estimationParameters) {
		if (this.estimationParameters == null) this.estimationParameters = new ArrayList<EstimationParameter>();
		this.estimationParameters.add(estimationParameters);
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
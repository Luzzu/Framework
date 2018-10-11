package io.github.luzzu.assessment.internalmetrics;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.core.Quad;
import io.github.luzzu.assessment.QualityMetric;
import io.github.luzzu.exceptions.MetricProcessingException;
import io.github.luzzu.qualityproblems.ProblemCollection;
import io.github.luzzu.semantics.vocabularies.LQM;

public class SyntaxErrorMetric implements QualityMetric<Boolean> {

	private String datasetURI = "";
	private Boolean hasErrors = false;
	
	@Override
	public void compute(Quad quad) throws MetricProcessingException { }

	
	public void setHasErrors(boolean hasSyntaxError) {
		this.hasErrors = hasSyntaxError;
	}
	
	
	@Override
	public Boolean metricValue() {
		return this.hasErrors.booleanValue();
	}

	@Override
	public Resource getMetricURI() {
		return LQM.SyntaxErrorsMetric;
	}

	@Override
	public ProblemCollection<?> getProblemCollection() {
		return null;
	}

	@Override
	public boolean isEstimate() {
		return false;
	}

	@Override
	public Resource getAgentURI() {
		return null;
	}

	@Override
	public void setDatasetURI(String datasetURI) {
		this.datasetURI = datasetURI;
	}

	@Override
	public String getDatasetURI() {
		return this.datasetURI;
	}

	@Override
	public Model getObservationActivity() {
		Model activity = ModelFactory.createDefaultModel();
		return activity;
	}

}

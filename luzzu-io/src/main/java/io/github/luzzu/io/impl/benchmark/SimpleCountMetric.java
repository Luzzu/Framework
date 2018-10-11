package io.github.luzzu.io.impl.benchmark;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.vocabulary.RDF;

import io.github.luzzu.assessment.QualityMetric;
import io.github.luzzu.exceptions.MetricProcessingException;
import io.github.luzzu.qualityproblems.ProblemCollection;
import io.github.luzzu.semantics.commons.ResourceCommons;
import io.github.luzzu.semantics.vocabularies.DAQ;

public class SimpleCountMetric implements QualityMetric<Integer> {

	private int count = 0;
	private String datasetURI = "";
	
	public void compute(Quad quad) throws MetricProcessingException {
		this.count++;
	}

	public Integer metricValue() {
		return this.count;
	}

	public Resource getMetricURI() {
		return ModelFactory.createDefaultModel().createResource("http://theme-e.adaptcentre.ie/lqm#CountMetric");
	}

	public ProblemCollection<?> getProblemCollection() {
		return null;
	}

	public boolean isEstimate() {
		return false;
	}

	public Resource getAgentURI() {
		return ModelFactory.createDefaultModel().createResource("http://luzzu.github.io/agent/TestAgent");
	}

	public void setDatasetURI(String datasetURI) {
		this.datasetURI = datasetURI;
	}

	public String getDatasetURI() {
		return this.datasetURI;
	}

	public Model getObservationActivity() {
		Model activity = ModelFactory.createDefaultModel();
		
		Resource mp = ResourceCommons.generateURI();
		activity.add(mp, RDF.type, DAQ.MetricProfile);
		activity.add(mp, DAQ.totalDatasetTriples, ResourceCommons.generateTypeLiteral(count));

		
		return activity;
	}

}

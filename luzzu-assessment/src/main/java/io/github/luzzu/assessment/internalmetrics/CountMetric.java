package io.github.luzzu.assessment.internalmetrics;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.core.Quad;
import io.github.luzzu.assessment.QualityMetric;
import io.github.luzzu.qualityproblems.ProblemCollection;
import io.github.luzzu.semantics.vocabularies.LQM;

public class CountMetric implements QualityMetric<Long> {

	private String datasetURI = "";
	private AtomicLong count = new AtomicLong(0);
	
	@Override
	public void compute(Quad quad) { 
		this.count.incrementAndGet();
	}

	public void setCount(long count) {
		this.count.set(count);
	}
	
	@Override
	public Long metricValue() {
		return this.count.get();
	}

	@Override
	public Resource getMetricURI() {
		return LQM.CountMetric;
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

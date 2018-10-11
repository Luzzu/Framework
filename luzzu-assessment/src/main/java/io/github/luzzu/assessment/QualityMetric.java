package io.github.luzzu.assessment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.core.Quad;

import io.github.luzzu.exceptions.MetricProcessingException;
import io.github.luzzu.qualityproblems.ProblemCollection;

/**
 * @author Jeremy Debattista
 * 
 */
public interface QualityMetric<T> {
	
	/**
	 * A special-purpose logger, aimed to write statistics about metric value calculations in a file, 
	 * specified in the configuration of the logging subsystem (log4j)
	 */
	final Logger statsLogger = LoggerFactory.getLogger(QualityMetric.class);

	/**
	 * This method should compute the metric.
	 * 
	 * @param The Quad <s,p,o,c> passed by the stream processor to the quality metric
	 */
	void compute(Quad quad) throws MetricProcessingException;

	/**
	 * @return the value computed by the Quality Metric
	 */
	T metricValue();

	/**
	 * @return returns the daQ URI of the Quality Metric
	 */
	Resource getMetricURI();
	
	/**
	 * Each quality metric can have a problem report, which will be serialised in a Semantic Format using the Luzzu
	 * Quality Problem Report ontology. A ProblemCollection<?> can be one of its subclasses: 
	 * ProblemCollectionModel, ProblemCollectionQuad or a ProblemCollectionResource.
	 * 
	 * @return returns a typed Problem Collection which will be used to create a "quality report" of the metric.
	 */
	ProblemCollection<?> getProblemCollection();
	
	/**
	 * @return returns true if the assessed metric returns an estimate result due to its probabilistic assessment technique (e.g. bloom filters)
	 */
	boolean isEstimate();
	
	
	/**
	 * An agent is required to keep provenance track.  We encourage the definition of an
	 * agent which can be accessible online, or as part of the metrics vocabulary.
	 * 
	 * @return returns the Agent URI that assessed the metric's observation
	 */
	Resource getAgentURI();
	
	/**
	 * Sets the Dataset's URI that is being assessed 
	 */
	void setDatasetURI(String datasetURI);
	
	/**
	 * @return dataset's URI that is being assessed.
	 */
	String getDatasetURI();
	
	/**
	 * Provenance and Profiling information can be provided for each metric being assessed.
	 * This is an optional method that could be implemented. The method should return null
	 * or an empty model if no information is to be given with the observation.
	 * 
	 * @return A JENA model with provenance and profiling information.
	 */
	Model getObservationActivity();
}

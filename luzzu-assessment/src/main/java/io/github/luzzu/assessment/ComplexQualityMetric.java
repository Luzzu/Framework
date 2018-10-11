package io.github.luzzu.assessment;

import io.github.luzzu.exceptions.AfterException;
import io.github.luzzu.exceptions.BeforeException;

/**
 * @author Jeremy Debattista
 * 
 * This interface extends the "simpler" Quality Metric.
 * This gives us the possibility of creating more complex quality metrics
 * which require further processing of the data in the dataset.
 * 
 */
public interface ComplexQualityMetric<T> extends QualityMetric<T> {
	
	/**
	 * Implement this method if the quality metric
	 * requires any pre-processing. 
	 * 
	 * If pre-processing is required, it should be done
	 * here rather than in the constructor.
	 * 
	 * @param a set of arguments
	 */
	 void before(Object... args) throws BeforeException;
	
	/**
	 * Implement this method if the quality metric
	 * requires any post-processing
	 * 
	 * @param a set of arguments
	 */
	void after(Object... args) throws AfterException;
}

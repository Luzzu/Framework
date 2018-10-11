package io.github.luzzu.io;

import java.util.List;

import io.github.luzzu.exceptions.LuzzuIOException;
import io.github.luzzu.exceptions.ProcessorNotInitialised;
import io.github.luzzu.io.helper.IOStats;

public interface IOProcessor {

	/**
	 * Initialise the io processor with the necessary in-memory objects and metrics
	 */
	void setUpProcess() throws LuzzuIOException;
	
	/**
	 * Process the dataset for quality assessment
	 * 
	 * @throws ProcessorNotInitialised
	 */
	void startProcessing() throws LuzzuIOException, InterruptedException;
	
	/**
	 * Cleans up memory from unused objects after processing is finished
	 * 
	 * @throws ProcessorNotInitialised
	 */
	void cleanUp() throws LuzzuIOException;
	
	/**
	 * A workflow initiating the whole assessment process.
	 * Such method usually executes the setUpProcess and startProcessing
	 * methods. 
	 * 
	 * @throws ProcessorNotInitialised
	 */
	void processorWorkFlow() throws LuzzuIOException, InterruptedException;
	
	
	/**
	 * Returns statistics related to the IO processor
	 * such as the number of processed statements
	 * 
	 * @return
	 * @throws ProcessorNotInitialised
	 */
	List<IOStats> getIOStats() throws LuzzuIOException;
	
	/**
	 * Cancels the metric assessment and closes all 
	 * open threads
	 * 
	 * 
	 * @return True if cancellation is successful
	 * @throws ProcessorNotInitialised
	 */
	void cancelMetricAssessment() throws LuzzuIOException;
	
	/**
	 * Retrieves the number of metrics that are initialised
	 * for a quality assessment
	 * 
	 * 
	 * @return number of metrics initalised
	 */
	int getNumberOfInitMetrics();
}

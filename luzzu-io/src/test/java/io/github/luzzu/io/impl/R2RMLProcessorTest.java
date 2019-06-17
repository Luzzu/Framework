package io.github.luzzu.io.impl;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.github.luzzu.exceptions.LuzzuIOException;
import io.github.luzzu.io.IOProcessor;
import io.github.luzzu.semantics.commons.ResourceCommons;
import io.github.luzzu.semantics.vocabularies.LMI;

public class R2RMLProcessorTest extends Assert {
	private IOProcessor processor = null;

	private String datasetPLD = "http://example.org";
	private String datasetLocation = this.getClass().getClassLoader().getResource("processor-datadumps/test-r2rml-mapping.ttl").toExternalForm();
	private Boolean generateReport = false;
	private Model metricConf = ModelFactory.createDefaultModel();

	@Before
	public void setUp() throws Exception {
		metricConf.add(metricConf.createStatement(ResourceCommons.generateURI(), LMI.metric, "io.github.luzzu.testing.metrics.SimpleCountMetric"));
		processor = new R2RMLProcessor(datasetPLD, datasetLocation, generateReport, metricConf);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void setUpProcessTest() throws LuzzuIOException {
		processor.setUpProcess();
		assertEquals(1, processor.getNumberOfInitMetrics());
	}

	@Test
	public void processingTest() {
		try {
			processor.setUpProcess();
			processor.startProcessing();
		} catch (LuzzuIOException | InterruptedException e) {
			e.printStackTrace();
		}

		long triplesProcessed = 0;
		try {
			triplesProcessed = processor.getIOStats().get(0).getTriplesProcessed();
		} catch (LuzzuIOException e) {
			e.printStackTrace();
		}

		assertEquals(1, triplesProcessed);
	}

}

package io.github.luzzu.io.impl;

import org.apache.jena.fuseki.embedded.FusekiServer;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.system.Txn;
import org.apache.jena.tdb.TDBFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.github.luzzu.exceptions.LuzzuIOException;
import io.github.luzzu.io.IOProcessor;
import io.github.luzzu.semantics.commons.ResourceCommons;
import io.github.luzzu.semantics.vocabularies.LMI;

public class SPARQLEndPointProcessorTest extends Assert {

	private IOProcessor processor = null;

	private String datasetPLD = "http://example.org";
	private String datasetLocation = this.getClass().getClassLoader().getResource("processor-datadumps/test-data-40377.nt").toExternalForm();
	private Boolean generateReport = false;
	private Model metricConf = ModelFactory.createDefaultModel();
	private FusekiServer server;

	@Before
	public void setUp() throws Exception {
		DatasetGraph dsg = TDBFactory.createDatasetGraph();
		server = FusekiServer.make(9876, "/ds", dsg);
		server.start();
		Txn.executeWrite(dsg, () -> RDFDataMgr.read(dsg, datasetLocation));

		metricConf.add(metricConf.createStatement(ResourceCommons.generateURI(), LMI.metric, "io.github.luzzu.testing.metrics.SimpleCountMetric"));
		processor = new SPARQLEndPointProcessor(datasetPLD, "http://localhost:9876/ds", generateReport, metricConf);
	}

	@After
	public void tearDown() throws Exception {
		server.stop();
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
		long expectedTriplesProcessed = 40377l;

		assertEquals(expectedTriplesProcessed, triplesProcessed);
	}

}

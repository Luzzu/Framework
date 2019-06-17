package io.github.luzzu.io.impl;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.github.luzzu.datatypes.r2rml.R2RMLMapping;
import io.github.luzzu.datatypes.r2rml.TriplesMap;
import io.github.luzzu.exceptions.LuzzuIOException;
import io.github.luzzu.semantics.commons.ResourceCommons;
import io.github.luzzu.semantics.vocabularies.LMI;

public class R2RMLProcessorTest extends Assert {
	private R2RMLProcessor processor = null;

	private String datasetPLD = "http://example.org";
	private String datasetLocation = this.getClass().getClassLoader().getResource("processor-datadumps/test-r2rml-mapping.ttl").toExternalForm();
	private Boolean generateReport = false;
	private Model metricConf = ModelFactory.createDefaultModel();

	@Before
	public void setUp() throws Exception {
		metricConf.add(metricConf.createStatement(ResourceCommons.generateURI(), LMI.metric, "io.github.luzzu.testing.metrics.ComplexR2RMLMetric"));
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

		assertNotNull(processor.r2rmlMappings);
		R2RMLMapping mapping = processor.r2rmlMappings.get(0);
		assertNotNull(mapping);
		TriplesMap triplesMap = mapping.getTriplesMaps().values().iterator().next();
		assertEquals("", triplesMap.getBaseIRI());
		assertEquals("city", triplesMap.getLogicalTable().getTableName());
		assertEquals("http://example.org/city/{city_id}", triplesMap.getSubjectMap().getTemplate());
		assertEquals("http://www.w3.org/2003/01/geo/wgs84_pos#SpatialThing", triplesMap.getSubjectMap().getClasses().iterator().next().getURI());
		assertEquals(1, triplesMap.getPredicateObjectMaps().size());
		assertEquals("http://www.w3.org/2000/01/rdf-schema#label", triplesMap.getPredicateObjectMaps().iterator().next().getPredicateMaps().iterator().next().getConstant().asNode().getURI());
		assertEquals("name", triplesMap.getPredicateObjectMaps().iterator().next().getObjectMaps().iterator().next().getColumn());
	}

}

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

public class MappingProcessorTest extends Assert {
	private R2RMLProcessor processor = null;

	private String datasetPLD = "http://example.org";
	private String datasetR2RMLLocation = this.getClass().getClassLoader().getResource("processor-datadumps/test-r2rml-mapping.ttl").toExternalForm();
	private String datasetRMLLocation = this.getClass().getClassLoader().getResource("processor-datadumps/test-rml-mapping.ttl").toExternalForm();
	private String datasetRML2 = this.getClass().getClassLoader().getResource("processor-datadumps/PubAuthGroup_Mapping.rml.ttl").toExternalForm();
	private Boolean generateReport = false;
	private Model metricConf = ModelFactory.createDefaultModel();

	@Before
	public void setUp() throws Exception {
		metricConf.add(metricConf.createStatement(ResourceCommons.generateURI(), LMI.metric, "io.github.luzzu.testing.metrics.ComplexR2RMLMetric"));
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void setUpProcessTest() throws LuzzuIOException {
		processor = new R2RMLProcessor(datasetPLD, datasetR2RMLLocation, generateReport, metricConf);
		processor.setUpProcess();
		assertEquals(1, processor.getNumberOfInitMetrics());
	}

	@Test
	public void processingR2RMLTest() {
		try {
			processor = new R2RMLProcessor(datasetPLD, datasetR2RMLLocation, generateReport, metricConf);
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

	@Test
	public void processingRMLTest() {
		try {
			processor = new R2RMLProcessor(datasetPLD, datasetRMLLocation, generateReport, metricConf);
			processor.setUpProcess();
			processor.startProcessing();
		} catch (Exception e) {
			e.printStackTrace();
		}

		assertNotNull(processor.r2rmlMappings);
		R2RMLMapping mapping = processor.r2rmlMappings.get(0);
		assertNotNull(mapping);
		TriplesMap triplesMap = mapping.getTriplesMaps().values().iterator().next();
		assertEquals("", triplesMap.getBaseIRI());
		assertEquals("student2.csv", triplesMap.getLogicalTable().getTableName());
		assertEquals("http://example.com/{ID}/{Name}", triplesMap.getSubjectMap().getTemplate());
		assertEquals("http://example.com/Student", triplesMap.getSubjectMap().getClasses().iterator().next().getURI());
		assertEquals(1, triplesMap.getPredicateObjectMaps().size());
		assertEquals("http://example.com/id", triplesMap.getPredicateObjectMaps().iterator().next().getPredicateMaps().iterator().next().getConstant().asNode().getURI());
		assertEquals("IDs", triplesMap.getPredicateObjectMaps().iterator().next().getObjectMaps().iterator().next().getColumn());
	}

}

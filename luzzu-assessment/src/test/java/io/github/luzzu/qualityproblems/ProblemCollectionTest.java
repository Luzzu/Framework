package io.github.luzzu.qualityproblems;

import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.vocabulary.RDF;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.github.luzzu.qualityproblems.old.ProblemCollectionQuadSlower;
import io.github.luzzu.semantics.commons.ResourceCommons;
import io.github.luzzu.semantics.vocabularies.QPRO;

public class ProblemCollectionTest extends Assert {
	
	Resource metric = ModelFactory.createDefaultModel().createResource("urn:MetricExample");
	
	
	@Before
	public void setUp() throws Exception {}
	
	@After
	public void tearDown() throws Exception{}
	
	@Test
	public void ProblemCollectionResourceTest() {
		ProblemCollection<Resource> problemCollection = new ProblemCollectionResource(metric);
		
		for (int i = 0; i < 10; i++) {
			problemCollection.addProblem(ResourceCommons.generateURI());
		}
		
		String namedGraph = problemCollection.getNamedGraph();
		assertNotNull(namedGraph);
		
		Dataset ds = problemCollection.getDataset();
		assertNotNull(ds);

		Model m = ds.getNamedModel(namedGraph);
		long expectedValue = 15l;
		ds.begin(ReadWrite.READ);
//		m.write(System.out,"TURTLE");
		long actualValue = m.size();
		assertEquals(expectedValue, actualValue); 
		problemCollection.cleanup();

	}

	@Test
	public void ProblemCollectionQuadsTest() {
		ProblemCollection<Quad> problemCollection = new ProblemCollectionQuadSlower(metric);
		
		for (int i = 0; i < 10; i++) {
			Quad q = new Quad(null, new Triple(ResourceCommons.generateURI().asNode(), ResourceCommons.generateURI().asNode(), ResourceCommons.generateURI().asNode()));
			problemCollection.addProblem(q);
		}
		((ProblemCollectionQuadSlower)problemCollection).commit();
		
		String namedGraph = problemCollection.getNamedGraph();
		assertNotNull(namedGraph);
		
		Dataset ds = problemCollection.getDataset();
		assertNotNull(ds);
		
		ds.begin(ReadWrite.READ);
		Model m = ds.getNamedModel(namedGraph);
//		m.write(System.out,"TURTLE");
		long expectedValue = 55l;
		long actualValue = m.size();
		assertEquals(expectedValue, actualValue); 
		problemCollection.cleanup();
	}
	
	@Test
	public void ProblemCollectionModelTest() {
		ProblemCollection<Model> problemCollection = new ProblemCollectionModel(metric);
		
		for (int i = 0; i < 10; i++) {
			Model m = ModelFactory.createDefaultModel();
			Resource n = ResourceCommons.generateURI();
			m.add(n, RDF.type, QPRO.Exception);
			m.add(n, QPRO.exceptionDescription, m.createLiteral("Description"));

			((ProblemCollectionModel)problemCollection).addProblem(m, ModelFactory.createDefaultModel().createResource());
		}
		
		String namedGraph = problemCollection.getNamedGraph();
		assertNotNull(namedGraph);
		Dataset ds = problemCollection.getDataset();
		assertNotNull(ds);

		Model m = ds.getNamedModel(namedGraph);
		long expectedValue = 33l;
		ds.begin(ReadWrite.READ);
//		m.write(System.out,"TURTLE");
		long actualValue = m.size();
		assertEquals(expectedValue, actualValue); 
		problemCollection.cleanup();

	}
}

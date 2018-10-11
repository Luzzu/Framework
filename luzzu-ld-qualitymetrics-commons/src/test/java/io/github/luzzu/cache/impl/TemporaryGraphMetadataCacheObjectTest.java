package io.github.luzzu.cache.impl;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.github.luzzu.operations.cache.CacheManager;
import io.github.luzzu.qualitymetrics.commons.cache.TemporaryGraphMetadataCacheObject;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.sparql.core.Quad;

public class TemporaryGraphMetadataCacheObjectTest extends Assert {
	

	private Model m = ModelFactory.createDefaultModel();
	private CacheManager mgr = CacheManager.getInstance();
	
	@Before
	public void setUp() throws Exception{
		
	}
	
	@Test
	public void createNewCache() {
		mgr.createNewCache("test", 10000, false);
		
		assertTrue(mgr.cacheExists("test"));
	}
	
	@Test
	public void createObjectInCache() {
		mgr.createNewCache("test2", 10000, true);
		
		TemporaryGraphMetadataCacheObject mdCObj = new TemporaryGraphMetadataCacheObject(m.createResource("urn:testMetadataGraph"));
		mdCObj.addTriplesToMetadata(Quad.create(m.createResource("urn:testMetadataGraph").asNode(), 
				m.createResource("urn:subject").asNode(),
				m.createResource("urn:predicate").asNode(),
				m.createResource("urn:object").asNode()));
		
		mgr.addToCache("test2", "obj1", mdCObj);
		
		TemporaryGraphMetadataCacheObject tmp = (TemporaryGraphMetadataCacheObject) mgr.getFromCache("test2", "obj1");
		
		assertEquals(1,tmp.getMetadataModel().size());
	}
}

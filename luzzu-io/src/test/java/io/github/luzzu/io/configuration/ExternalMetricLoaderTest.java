package io.github.luzzu.io.configuration;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.github.luzzu.io.configuration.ExternalMetricLoader;

public class ExternalMetricLoaderTest extends Assert {

	private ExternalMetricLoader loader = null;
	
	@Before
	public void setUp() throws Exception {
		loader = ExternalMetricLoader.getInstance();
	}
	
	@After
	public void tearDown() throws Exception{ }
	
	@Test
	public void getQualityMetricClassesTest() {
		assertEquals(3,loader.getQualityMetricClasses().size());
	}
	
	
}

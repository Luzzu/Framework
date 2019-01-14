package io.github.luzzu.io.configuration;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.github.luzzu.assessment.QualityMetric;
import io.github.luzzu.exceptions.MetricProcessingException;
import io.github.luzzu.io.configuration.DeclerativeMetricCompiler;
import io.github.luzzu.qml.parser.ParseException;
import io.github.luzzu.semantics.commons.ResourceCommons;

public class DeclerativeMetricCompilerTest extends Assert {

	DeclerativeMetricCompiler metC;
	
	@Before
	public void setUp() throws Exception {
		metC = DeclerativeMetricCompiler.getInstance();
	}
	
	@AfterClass
	public static void tearDown() throws Exception {
		FileUtils.deleteDirectory(new File("classes/"));
	}
	
	@Test
	public void successfulCompilationTest() throws IOException, ParseException {
		assertEquals(2,metC.compile().size());
	}
	

	@Test
	public void successfulExecutionTest() throws IOException, ParseException, 
		MetricProcessingException, InstantiationException, IllegalAccessException {
		Map<String, Class<? extends QualityMetric<?>>> clz = metC.compile();
		
		for(Class<? extends QualityMetric<?>> clazz : clz.values()){
			QualityMetric<?> metric = clazz.newInstance();
			metric.compute(new Quad(null, new Triple(ResourceCommons.generateURI().asNode(), FOAF.name.asNode(), ResourceCommons.generateURI().asNode())));
			
			Double actual = (Double) metric.metricValue();
			Double expected = 1.0d;
			assertEquals(expected,actual);
		}
		
		
	}
	
}

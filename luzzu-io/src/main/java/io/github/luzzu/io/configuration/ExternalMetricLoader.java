package io.github.luzzu.io.configuration;

import java.io.File;
import java.io.FileFilter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.NodeIterator;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.ResIterator;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.RDF;

import io.github.luzzu.assessment.QualityMetric;
import io.github.luzzu.datatypes.Args;
import io.github.luzzu.operations.lowlevel.ExceptionOutput;
import io.github.luzzu.semantics.vocabularies.LMI;


/**
 * @author Jeremy Debattista
 * 
 * The class handles the loading of External Quality Metrics.
 * 
 */
public class ExternalMetricLoader {

	final static Logger logger = LoggerFactory.getLogger(ExternalMetricLoader.class);
	private static ExternalMetricLoader instance = null;
	private static ConcurrentMap<File,List<String>> metricsInFile = new ConcurrentHashMap<File,List<String>>(); // Jar File, List of Metrics
	
	private static ConcurrentMap<String, List<Args>> beforeArgsMap = new ConcurrentHashMap<String, List<Args>>();
	private static ConcurrentMap<String, List<Args>> afterArgsMap = new ConcurrentHashMap<String, List<Args>>();

	
	private static FileFilter jarFilter = new FileFilter() {
		public boolean accept(File file) {
			if (file.getName().endsWith(".jar")) {
				return true;
			}
			return false;
		}
	};
	
	private static FileFilter pomFilter = new FileFilter() {
		public boolean accept(File file) {
			if (file.getName().equals("pom.xml")) {
				return true;
			}
			return false;
		}
	};
	
	protected ExternalMetricLoader() {}
	
	public static ExternalMetricLoader getInstance(){
		if (instance == null) {
			logger.info("Initialising and verifying external metrics.");
			instance = new ExternalMetricLoader();
			loadMetrics();	
		}
		
		return instance;
	}
	
	private static void loadMetrics(){
		File externalsFolder = new File("externals/metrics/");
		File[] listOfFiles = externalsFolder.listFiles();
		
		for(File metrics : listOfFiles){
			if (metrics.isHidden()) continue;
			if (!metrics.isDirectory()) continue;
			
			//If we have a POM file then we should load dependencies
			File[] pomFiles = metrics.listFiles(pomFilter);
			if (pomFiles != null && pomFiles.length > 0 && pomFiles[0] != null)  {
				DependencyLoader dl = new DependencyLoader(pomFiles[0].toPath().toString());
				dl.resolve();
			}
			
			File[] jarFiles = metrics.listFiles(jarFilter);
			if (jarFiles.length == 0)  {
				continue;
			}
			File jarFile = jarFiles[0];
			metricsInFile.putIfAbsent(jarFile, new ArrayList<String>());
			logger.debug("Loading metrics from : {} ", jarFile.toPath());
			
			Model m = ModelFactory.createDefaultModel();
			m.read(metrics+"/metrics.trig");
			
			
			ResIterator res = m.listSubjectsWithProperty(RDF.type, LMI.LuzzuMetricJavaImplementation);
			while (res.hasNext()){
				Resource r = res.next();
				NodeIterator n = m.listObjectsOfProperty(r, LMI.javaPackageName);
				
				String javaMetric = n.next().toString();
				metricsInFile.get(jarFile).add(javaMetric);
				logger.debug("Metric : {} ", javaMetric); 
				
				n = m.listObjectsOfProperty(r, LMI.before);
				List<Args> beforeArgs = new ArrayList<Args>();
				while (n.hasNext()){
					RDFNode before = n.next();
					String type = m.listObjectsOfProperty(before.asResource(), LMI.type).next().asLiteral().getString();
					String param = m.listObjectsOfProperty(before.asResource(), LMI.parameter).next().asLiteral().getString();
					Args a = new Args();
					a.setParameter(param);
					a.setType(type);
					beforeArgs.add(a);
				}
				beforeArgsMap.put(javaMetric, beforeArgs);
				
				List<Args> afterArgs = new ArrayList<Args>();
				n = m.listObjectsOfProperty(r, LMI.after);
				if (n.hasNext()){
					RDFNode after = n.next();
					String type = m.listObjectsOfProperty(after.asResource(), LMI.type).next().asLiteral().getString();
					String param = m.listObjectsOfProperty(after.asResource(), LMI.parameter).next().asLiteral().getString();
					Args a = new Args();
					a.setParameter(param);
					a.setType(type);
					afterArgs.add(a);
				}
				afterArgsMap.put(javaMetric, afterArgs);
			}
		}
	}
	
	@SuppressWarnings({ "resource", "unchecked" })
	public Map<String, Class<? extends QualityMetric<?>>> getQualityMetricClasses() {
		if (instance == null) getInstance();
		Map<String, Class<? extends QualityMetric<?>>> clazzes = new HashMap<String,Class<? extends QualityMetric <?>>>();
		
		for(File jar : metricsInFile.keySet()){
			List<String> metrics = metricsInFile.get(jar);
			
			URL jarUrl = null;
			try {
				jarUrl = new URL("file:" + jar.getAbsolutePath());
			} catch (MalformedURLException e) {
				Exception e2 = new MalformedURLException("Jar URL is malformed "+jarUrl+" Skipped loading the file ");
				ExceptionOutput.output(e2, "[External Metric Loader] Malformed Jar URL.", logger);
				continue;
			}
			URLClassLoader cl = new URLClassLoader(new URL[] {jarUrl}, ExternalMetricLoader.class.getClassLoader());
			
			for(String m : metrics){
				logger.info("Creating class for metric : {} ", m);
				Class<? extends QualityMetric<?>> clazz;
				try {
					clazz = (Class<? extends QualityMetric<?>>) cl.loadClass(m);
				} catch (ClassNotFoundException e) {
					Exception e2 = new ClassNotFoundException("Class "+m+" is not found for External JAR package "+jar.getName()+". Skipped loading the class.", e);
					ExceptionOutput.output(e2, "[External Metric Loader] Class not found.", logger);
					continue;
				}
				clazzes.put(m, clazz);
			}
		}
		return clazzes;
	}
	
	public List<Args> getBeforeArgs(String className){
		return beforeArgsMap.get(className);
	}
	
	public List<Args> getAfterArgs(String className){
		return afterArgsMap.get(className);
	}
}

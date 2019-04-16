package io.github.luzzu.semantics.configuration;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.luzzu.operations.lowlevel.ExceptionOutput;
import io.github.luzzu.semantics.vocabularies.DAQ;
import io.github.luzzu.semantics.vocabularies.LQM;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.util.FileUtils;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDF;

public class InternalModelConf {

	// creates an empty model for the default dataset - a dataset is readonly.
	private static Dataset semanticModel = DatasetFactory.create(ModelFactory
			.createDefaultModel());

	final static Logger logger = LoggerFactory
			.getLogger(InternalModelConf.class);

	static {
		// Loading DAQ ontology into memory
		Model temp = ModelFactory.createDefaultModel();
		
		temp.read(InternalModelConf.class.getResourceAsStream("/vocabularies/daq/daq.ttl"), null, "N3");
		semanticModel.addNamedModel(DAQ.NS, temp);

		temp = ModelFactory.createDefaultModel();
		temp.read(InternalModelConf.class.getResourceAsStream("/vocabularies/lqm/lqm.ttl"), null, "N3");
		semanticModel.addNamedModel(LQM.NS, temp);


		File externalsFolder = new File("externals/vocabs/");
		if (externalsFolder.exists()){
			File[] listOfOntologies = externalsFolder.listFiles();
			for (File ontology : listOfOntologies) {
				if (FileUtils.guessLang(ontology.getPath(), null) == null) continue;
				if (ontology.isHidden()) continue;
				temp = ModelFactory.createDefaultModel();
				logger.info("Loading ontology : {} ", ontology.getName());
				temp.read(ontology.getPath(), "N3");
				try{
					String namespace = guessNamespace(temp);
					if (namespace != null)
						semanticModel.addNamedModel(namespace, temp);
					else
						semanticModel.getDefaultModel().add(temp);
				} catch (Exception e) {
					logger.error("Could not load model " + ontology.getPath());
				}
			}	
		} else {
			ExceptionOutput.output(new FileNotFoundException("Cannot find vocabs folder in "+externalsFolder.getPath()), "[InternalModelConf] - Missing Vocabulary Folder", logger);
		}
	}

	private static String guessNamespace(Model temp) {
		List<Resource> res = temp.listSubjectsWithProperty(RDF.type, OWL.Ontology).toList();
		Map<String, Integer> tempMap = new HashMap<String, Integer>();
		for (Resource r : res) {
			String ns = r.getNameSpace();
			tempMap.put(ns, (tempMap.containsKey(ns)) ? (tempMap.get(ns) + 1) : 1);
		}
		
		if (tempMap.size() > 0)
			return (String) sortByValue(tempMap).keySet().toArray()[0];
		else
			return null;
	}

	public static Model getDAQModel() {
		return semanticModel.getNamedModel(DAQ.NS);
	}


	public static Model getFlatModel() {
		Model m = ModelFactory.createDefaultModel();
		
		Iterator<String> iter = semanticModel.listNames();
		
		while (iter.hasNext()){
			m.add(semanticModel.getNamedModel(iter.next()));
		}
		m.add(semanticModel.getDefaultModel());
		return m;
	}

	public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
		List<Map.Entry<K, V>> list = new LinkedList<Map.Entry<K, V>>(
				map.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
			public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
				return (o1.getValue()).compareTo(o2.getValue());
			}
		});

		Map<K, V> result = new LinkedHashMap<K, V>();
		for (Map.Entry<K, V> entry : list) {
			result.put(entry.getKey(), entry.getValue());
		}
		return result;
	}	
}

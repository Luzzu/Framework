package io.github.luzzu.web.assessment;

import java.io.File;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.NodeIterator;
import org.apache.jena.rdf.model.ResIterator;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.luzzu.semantics.commons.ResourceCommons;
import io.github.luzzu.semantics.configuration.InternalModelConf;
import io.github.luzzu.semantics.vocabularies.DAQ;
import io.github.luzzu.semantics.vocabularies.LMI;
import io.github.luzzu.web.ranking.Facets;

/**
 * @author Jeremy Debattista
 *
 * This class deals with metric setting vis-a-vie
 * the Web UI communication e.g. getting a list 
 * of all installed metrics
 * 
 */
public class MetricConfiguration {
	
	final static Logger logger = LoggerFactory.getLogger(Facets.class);
	
	private static Model internalModel = InternalModelConf.getFlatModel();

	public static Model getAllMetrics(){
		File externalsFolder = new File("externals/metrics/");
		File[] listOfFiles = externalsFolder.listFiles();
		
		Model returnModel = ModelFactory.createDefaultModel();
		
		if (listOfFiles == null) {
			logger.info("No files available");
			return returnModel;
		}
		
		for(File metrics : listOfFiles){
			if (metrics.isHidden()) continue;
			if (!metrics.isDirectory()) continue;
			
			//If we have a POM file then we should load dependencies
			Model m = ModelFactory.createDefaultModel();
			m.read(metrics+"/metrics.trig");
			
			ResIterator res = m.listSubjectsWithProperty(RDF.type, LMI.LuzzuMetricJavaImplementation);
			while (res.hasNext()){
				Resource r = res.next();
				String jpn = m.listObjectsOfProperty(r, LMI.javaPackageName).next().asLiteral().toString();
				
				NodeIterator n = m.listObjectsOfProperty(r, RDFS.label);
				String label = "";
				if (n.hasNext()) label = n.next().asLiteral().toString();
				
				Resource metric = m.listObjectsOfProperty(r, LMI.referTo).next().asResource();
				NodeIterator comment = internalModel.listObjectsOfProperty(metric, RDFS.comment);
				String metricComment = "";
				if (comment.hasNext()) metricComment = comment.next().asLiteral().getString();
				
				Resource expectedDataType = internalModel.listObjectsOfProperty(metric, DAQ.expectedDataType).nextNode().asResource();
				
				Resource bn = ResourceCommons.generateRDFBlankNode().asResource();
				returnModel.add(bn, LMI.javaPackageName, returnModel.createLiteral(jpn));
				returnModel.add(bn, RDFS.label, returnModel.createLiteral(label));
				returnModel.add(bn, LMI.referTo, metric);
				returnModel.add(bn, DAQ.expectedDataType, expectedDataType);
				if (metricComment != "") returnModel.add(bn, RDFS.comment, returnModel.createLiteral(metricComment));
			}
		}
		return returnModel;
	}
}

package io.github.luzzu.web.export;

import java.util.Map;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.luzzu.operations.ranking.DatasetLoader;
import io.github.luzzu.semantics.dqv.ConvertDAQ;

public class MetadataExport {
	final static Logger logger = LoggerFactory.getLogger(MetadataExport.class);


	private static Dataset d = DatasetLoader.getInstance().getInternalDataset();
	private static Map<String, String> datasets_metadata = DatasetLoader.getInstance().getAllGraphs();
	
	public static Dataset exportToDQV(String datasetPLD) {
		datasetPLD = datasetPLD.replace("http://", "");
		datasetPLD = datasetPLD.replace("https://", "");
		datasetPLD = datasetPLD.replace("uri:", "");
		datasetPLD = datasetPLD.replace(":", "_");
		
		String graph = datasets_metadata.get(datasetPLD);
		Model m = d.getNamedModel(graph);
		
		Model defModel = ModelFactory.createDefaultModel();
		defModel.add(d.getDefaultModel().listStatements(m.createResource(graph), (Property) null, (RDFNode) null).toList());
		
		Dataset d = DatasetFactory.create();
		d.addNamedModel(graph, m);
		d.getDefaultModel().add(defModel);
		
		Dataset dqv = ConvertDAQ.convert(d);
		return dqv;
	}
	
	public static Dataset exportToDAQ(String datasetPLD) {
		datasetPLD = datasetPLD.replace("http://", "");
		datasetPLD = datasetPLD.replace("https://", "");
		datasetPLD = datasetPLD.replace("uri:", "");
		datasetPLD = datasetPLD.replace(":", "_");
		
		String graph = datasets_metadata.get(datasetPLD);
		Model m = d.getNamedModel(graph);
		
		Model defModel = ModelFactory.createDefaultModel();
		defModel.add(d.getDefaultModel().listStatements(m.createResource(graph), (Property) null, (RDFNode) null).toList());
		
		Dataset daq = DatasetFactory.create();
		daq.addNamedModel(graph, m);
		daq.getDefaultModel().add(defModel);
		
		return daq;
	}
	

}

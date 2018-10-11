package io.github.luzzu.semantics.dqv;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.io.FileUtils;
import org.apache.jena.ext.com.google.common.base.Predicate;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.InfModel;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.impl.StmtIteratorImpl;
import org.apache.jena.reasoner.Reasoner;
import org.apache.jena.reasoner.rulesys.GenericRuleReasonerFactory;
import org.apache.jena.util.PrintUtil;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.vocabulary.ReasonerVocabulary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.luzzu.operations.lowlevel.ExceptionOutput;
import io.github.luzzu.operations.properties.PropertyManager;
import io.github.luzzu.semantics.configuration.InternalModelConf;
import io.github.luzzu.semantics.vocabularies.DAQ;

/**
 * This class uses a reasoner to convert a daQ file into W3C DQV
 * For more information about DQV check {@link https://www.w3.org/TR/vocab-dqv/}
 * 
 * @author Jeremy Debattista
 */
public class ConvertDAQ {
	
	final static Logger logger = LoggerFactory.getLogger(ConvertDAQ.class);
	
	public static Dataset convert(Dataset metadata) {
		PrintUtil.registerPrefix("daq", DAQ.NS);
		PrintUtil.registerPrefix("dqv", "http://www.w3.org/ns/dqv#");
		PrintUtil.registerPrefix("qb", "http://purl.org/linked-data/cube#");
		
		Model daqData = metadata.getNamedModel(metadata.listNames().next());
		daqData.add(InternalModelConf.getFlatModel());
		
		String fileloc = "";
		try {
			InputStream is = ConvertDAQ.class.getResourceAsStream("/rules/daq2dqv.rules");
			String tmpLoc = PropertyManager.getInstance().getProperties("luzzu.properties").getProperty("FILE_TEMP_BASE_DIR");
			File tempFile = File.createTempFile("daq2dqv", ".rules", new File(tmpLoc));
			FileUtils.copyInputStreamToFile(is, tempFile);
			fileloc = tempFile.getAbsolutePath();
		} catch (IOException e) {
			ExceptionOutput.output(e, "IO Exception thrown on DAQ Converter", logger);
		}
		
		Model m = ModelFactory.createDefaultModel();
		Resource configuration =  m.createResource();
		configuration.addProperty(ReasonerVocabulary.PROPruleMode, "hybrid");
		configuration.addProperty(ReasonerVocabulary.PROPruleSet,  fileloc);
		
		Reasoner reasoner = GenericRuleReasonerFactory.theInstance().create(configuration);

		InfModel data = ModelFactory.createInfModel(reasoner, daqData);
		ExtendedIterator<Statement> datastmts = data.listStatements().filterKeep(new Predicate<Statement>(){
	        @Override
	        public boolean apply(Statement o) {
	        		if (daqData.contains(o)) return false;
	        		return true;
	        }
		});
		
		InfModel graph = ModelFactory.createInfModel(reasoner, metadata.getDefaultModel());
		ExtendedIterator<Statement> graphstmts = graph.listStatements().filterKeep(new Predicate<Statement>(){
	        @Override
	        public boolean apply(Statement o) {
	        		if (metadata.getDefaultModel().contains(o)) return false;
	        		return true;
	        }
		});

		Model deductions = ModelFactory.createDefaultModel().add( new StmtIteratorImpl( datastmts ));
		Model graphDed = ModelFactory.createDefaultModel().add( new StmtIteratorImpl( graphstmts ));

		Dataset metadataDQV = DatasetFactory.create();
		metadataDQV.getDefaultModel().add(graphDed);
		metadataDQV.addNamedModel(metadata.listNames().next(), deductions);
		
		
		FileUtils.deleteQuietly(new File(fileloc));
		
		return metadataDQV;
	}
}

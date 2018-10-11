package io.github.luzzu.annotations;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.impl.StatementImpl;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.vocabulary.RDF;

import io.github.luzzu.qualityproblems.ProblemCollection;
import io.github.luzzu.semantics.commons.ResourceCommons;
import io.github.luzzu.semantics.vocabularies.QPRO;

/**
 * @author Jeremy Debattista
 * 
 * The ProblemReport Class provides a number of methods
 * to enable the representation of problematic triples 
 * found during the assessment of Linked Datasets.
 * This class describes these problematic triples in
 * terms of either Refied RDF or a Sequence of Resources.
 * The Quality Report description can be found in
 * @see src/main/resource/vocabularies/QPRO/QPRO.trig
 * 
 */
public class ProblemReport {
	
	private Model m = ModelFactory.createDefaultModel();
	private Resource reportURI;
	
	private File serialisationFile;
	private OutputStream serialisationOutput;

	@Deprecated
	public ProblemReport(Resource computedOn){
		reportURI = ResourceCommons.generateURI();
		m.add(new StatementImpl(reportURI, RDF.type, QPRO.QualityReport));
		m.add(new StatementImpl(reportURI, QPRO.computedOn, computedOn));
	}
	
	public ProblemReport(Resource computedOn, File file) throws IOException{
		reportURI = ResourceCommons.generateURI();
		m.add(new StatementImpl(reportURI, RDF.type, QPRO.QualityReport));
		m.add(new StatementImpl(reportURI, QPRO.computedOn, computedOn));
		
		this.serialisationFile = file;
		this.serialisationFile.createNewFile();
		this.serialisationOutput = new DataOutputStream(new BufferedOutputStream(
		        Files.newOutputStream(
		                Paths.get(this.serialisationFile.getPath()), StandardOpenOption.APPEND
		            )
		        ));
	}
	
	
	/**
	 * Create instance triples corresponding towards a Quality Report
	 * 
	 * @param computedOn - The resource URI of the dataset computed on
	 * @param problemCollection - A list of quality problem collections
	 * 
	 */
	public void addToQualityProblemReport(ProblemCollection<?> problemCollection){
		Resource problemURI = problemCollection.getProblemURI();
		this.m.add(new StatementImpl(this.reportURI, QPRO.hasProblem, problemURI));
		
		Dataset d = problemCollection.getDataset();

		try {
			d.begin(ReadWrite.READ);
			problemCollection.getReentrantLock().lock();
			Model _m = d.getNamedModel(problemCollection.getNamedGraph());
			RDFDataMgr.write(this.serialisationOutput, _m, RDFFormat.TURTLE_PRETTY);
		} finally {
			problemCollection.getReentrantLock().unlock();
			d.end();
			d.close();
		}
		problemCollection.cleanup();
	}
	

	@Deprecated
	public void serialiseToFile(File file) throws IOException {
		file.createNewFile();
		OutputStream out = new FileOutputStream(file, false);
		RDFDataMgr.write(out, m, RDFFormat.TURTLE_PRETTY);
	}
	
	public void closeSerialisedFile() throws IOException {
		RDFDataMgr.write(this.serialisationOutput, m, RDFFormat.TURTLE_PRETTY);
		this.serialisationOutput.close();
	}
	
	public void cleanup() {
		m = null;
		System.gc();
	}
}

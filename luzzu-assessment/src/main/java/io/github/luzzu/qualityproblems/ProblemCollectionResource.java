package io.github.luzzu.qualityproblems;

import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Seq;
import org.apache.jena.rdf.model.impl.StatementImpl;
import org.apache.jena.vocabulary.RDF;

import io.github.luzzu.semantics.commons.ResourceCommons;
import io.github.luzzu.semantics.vocabularies.QPRO;

public class ProblemCollectionResource extends ProblemCollection<Resource> {

	private Seq problemList;
	
	private boolean isEmpty = true;

	private int seqCounter = 1;
	
	private Model _m;
	private int totalTriples = 0;
	private final int MAX_TRIPLES = 1000000;
	
	public ProblemCollectionResource(Resource metricURI) {
		super(metricURI);
		
		dataset.begin(ReadWrite.WRITE) ;
		try {
			dataset.addNamedModel(this.namedGraph, ModelFactory.createDefaultModel());
			dataset.commit();
		} finally { 
			dataset.end() ; 
		}
		
		if (super.isHPCEnabled) {
			this._m = dataset.getNamedModel(this.namedGraph);
		} else {
			this._m = ModelFactory.createDefaultModel();
		}
		
		this.problemList =  this._m.createSeq();
		
		this._m.add(new StatementImpl(this.problemURI, RDF.type, QPRO.QualityProblem));
		this._m.add(new StatementImpl(this.problemURI, QPRO.isDescribedBy, metricURI));
		this._m.add(new StatementImpl(this.problemURI, QPRO.problemStructure, QPRO.ResourceContainer));
		this._m.add(new StatementImpl(this.problemURI, QPRO.problematicThing, this.problemList));

		totalTriples+= 4;
	}

	@Override
	public void addProblem(Resource problematicElement) {
		this.isEmpty = false;

		this._m.add(new StatementImpl(this.problemList, RDF.li(seqCounter), ResourceCommons.asRDFNode(problematicElement.asNode())));
		seqCounter++;
		totalTriples++;
		
		if (this.totalTriples >= MAX_TRIPLES) {
			this.commit();
		}
	}

	@Override
	public boolean isEmpty() {
		return this.isEmpty;
	}

	@Override
	public synchronized void commit() {
		if (super.isHPCEnabled) {
			// nothing to do here
		} else {
			if (this.totalTriples > 0) {
				logger.info("Commiting in-mem Model Resource to Datagraph. Metric: "+super.metricURI+". Number of triples added: "+this.totalTriples+" dataset location: "+super.location);
				
				this.totalTriples = 0;
				
				dataset.begin(ReadWrite.WRITE) ;
				try {
					super.getReentrantLock().lock();
					dataset.addNamedModel(getNamedGraph(), this._m);
				} finally { 
					dataset.commit();
					dataset.end();
					super.getReentrantLock().unlock();
				}
				
			    this._m.removeAll();
			    this._m = null;
			    System.gc();
			    this._m = ModelFactory.createDefaultModel();		
			}
		}
	}
}

package io.github.luzzu.qualityproblems;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.impl.StatementImpl;
import org.apache.jena.vocabulary.RDF;

import io.github.luzzu.semantics.vocabularies.QPRO;

public class ProblemCollectionModel extends ProblemCollection<Model> {

	private boolean isEmpty = true;
	
	private Model _m;
	private int totalTriples = 0;
	private final int MAX_TRIPLES = 1000000;

	
	public ProblemCollectionModel(Resource metricURI) {
		super(metricURI);
		dataset.begin(ReadWrite.WRITE) ;
		try {
			dataset.addNamedModel(this.namedGraph, ModelFactory.createDefaultModel());
			dataset.commit();
		} finally { 
			dataset.end(); 
		}
		
		if (super.isHPCEnabled) {
			this._m = dataset.getNamedModel(this.namedGraph);
		} else {
			this._m = ModelFactory.createDefaultModel();
		}
		
		this._m.add(new StatementImpl(this.problemURI, RDF.type, QPRO.QualityProblem));
		this._m.add(new StatementImpl(this.problemURI, QPRO.isDescribedBy, metricURI));
		this._m.add(new StatementImpl(this.problemURI, QPRO.problemStructure, QPRO.ModelContainer));
		totalTriples+= 3;
	}

	@Override
	public void addProblem(Model problematicElement) {
			throw new NotImplementedException("addProblem(Model problematicElement) is not implemented yet. Use the addProblem(Model problematicElement, Resource problematicThingURI) method");
	}

	/**
	 * The method ensures that the correct problematic thing URI is used during the report generation
	 * @param problematicElement
	 * @param problematicThingURI
	 */
	public void addProblem(Model problematicElement, Resource problematicThingURI) {
		this.isEmpty = false;

		this._m.add(new StatementImpl(this.problemURI, QPRO.problematicThing, problematicThingURI));
		this._m.add(problematicElement);
		
		totalTriples += (int) (problematicElement.size() + 1);
		if (this.totalTriples >= MAX_TRIPLES) {
			this.commit();
			totalTriples = 0;
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
				logger.info("Commiting in-mem Model to Datagraph. Metric: "+super.metricURI+". Number of triples added: "+this.totalTriples+" dataset location: "+super.location);
	
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

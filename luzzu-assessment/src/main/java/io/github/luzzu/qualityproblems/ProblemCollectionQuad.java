package io.github.luzzu.qualityproblems;

import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Seq;
import org.apache.jena.rdf.model.impl.StatementImpl;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.vocabulary.RDF;

import io.github.luzzu.semantics.commons.ResourceCommons;
import io.github.luzzu.semantics.vocabularies.QPRO;

public class ProblemCollectionQuad extends ProblemCollection<Quad> {

	private Seq problemList;
	private boolean isEmpty = true;
	
	private Model _m;
	private int totalTriples = 0;
	private final int MAX_TRIPLES = 1000000;
	
	private int seqCounter = 1;
	
	public ProblemCollectionQuad(Resource metricURI) {
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
		
		this.problemList =  this._m.createSeq();
		
		this._m.add(new StatementImpl(this.problemURI, RDF.type, QPRO.QualityProblem));
		this._m.add(new StatementImpl(this.problemURI, QPRO.isDescribedBy, metricURI));
		this._m.add(new StatementImpl(this.problemURI, QPRO.problemStructure, QPRO.QuadContainer));
		this._m.add(new StatementImpl(this.problemURI, QPRO.problematicThing, this.problemList));

		totalTriples+= 4;
	}

	@Override
	public void addProblem(Quad problematicElement) {	
		this.isEmpty = false;
		
		Resource bNode = ResourceCommons.generateRDFBlankNode().asResource();
		
		Quad q = problematicElement;
		this._m.add(new StatementImpl(bNode, RDF.type, RDF.Statement));
		this._m.add(new StatementImpl(bNode, RDF.subject, ResourceCommons.asRDFNode(q.getSubject())));
		this._m.add(new StatementImpl(bNode, RDF.predicate, ResourceCommons.asRDFNode(q.getPredicate())));
		this._m.add(new StatementImpl(bNode, RDF.object, ResourceCommons.asRDFNode(q.getObject())));
		
		if (q.getGraph() != null){
			_m.add(new StatementImpl(bNode, QPRO.inGraph, ResourceCommons.asRDFNode(q.getGraph())));
		}
		
		_m.add(new StatementImpl(this.problemList, RDF.li(seqCounter), ResourceCommons.asRDFNode(bNode.asNode())));
		seqCounter++;
		totalTriples+=5;
		
		if (this.totalTriples >= MAX_TRIPLES) {
			this.commit();
		}
	}

	@Override
	public boolean isEmpty() {
		return this.isEmpty;
	}
	
	public synchronized void commit() {
		if (super.isHPCEnabled) {
			// nothing to do here
		} else {
			if (this.totalTriples > 0) {
				logger.info("Commiting in-mem Model Quads to Datagraph. Metric: "+super.metricURI+". Number of triples added: "+this.totalTriples+" dataset location: "+super.location);
				
				this.totalTriples = 0;
		
				dataset.begin(ReadWrite.WRITE) ;
				try {
					dataset.addNamedModel(getNamedGraph(), this._m);
					dataset.commit();
				}  finally {
					dataset.end();
				}
				
			    this._m = null;
			    System.gc();
			    this._m = ModelFactory.createDefaultModel();
			    
			}
		}
	}

}

package io.github.luzzu.qualityproblems.old;

import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Seq;
import org.apache.jena.rdf.model.impl.StatementImpl;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.vocabulary.RDF;

import io.github.luzzu.qualityproblems.ProblemCollection;
import io.github.luzzu.semantics.commons.ResourceCommons;
import io.github.luzzu.semantics.vocabularies.QPRO;

public class ProblemCollectionQuadSlower extends ProblemCollection<Quad> {

	private Seq problemList;
	private boolean isEmpty = true;

	
	
	private int seqCounter = 1;
	
	public ProblemCollectionQuadSlower(Resource metricURI) {
		super(metricURI);
		
		dataset.begin(ReadWrite.WRITE) ;
		try {
			dataset.addNamedModel(this.namedGraph, ModelFactory.createDefaultModel());
			dataset.commit();
		} finally { 
			dataset.end() ; 
		}
		
		dataset.begin(ReadWrite.WRITE) ;
		try {
			Model m = dataset.getNamedModel(this.namedGraph);
			this.problemList =  m.createSeq();
			
			m.add(new StatementImpl(this.problemURI, RDF.type, QPRO.QualityProblem));
			m.add(new StatementImpl(this.problemURI, QPRO.isDescribedBy, metricURI));
			m.add(new StatementImpl(this.problemURI, QPRO.problemStructure, QPRO.QuadContainer));
			m.add(new StatementImpl(this.problemURI, QPRO.problematicThing, this.problemList));

			dataset.commit();
		} finally { 
			dataset.end() ; 
		}
	}

	@Override
	public void addProblem(Quad problematicElement) {	
		dataset.begin(ReadWrite.WRITE) ;
		try {
			this.isEmpty = false;
			
			Model m = dataset.getNamedModel(this.namedGraph);
			
			Resource bNode = ResourceCommons.generateRDFBlankNode().asResource();
			
			Quad q = problematicElement;
			m.add(new StatementImpl(bNode, RDF.type, RDF.Statement));
			m.add(new StatementImpl(bNode, RDF.subject, ResourceCommons.asRDFNode(q.getSubject())));
			m.add(new StatementImpl(bNode, RDF.predicate, ResourceCommons.asRDFNode(q.getPredicate())));
			m.add(new StatementImpl(bNode, RDF.object, ResourceCommons.asRDFNode(q.getObject())));
			
			if (q.getGraph() != null){
				m.add(new StatementImpl(bNode, QPRO.inGraph, ResourceCommons.asRDFNode(q.getGraph())));
			}
			
			m.add(new StatementImpl(this.problemList, RDF.li(seqCounter), ResourceCommons.asRDFNode(bNode.asNode())));
			seqCounter++;
			
			dataset.commit();
		} finally { 
			dataset.end() ; 
		}
	}

	@Override
	public boolean isEmpty() {
		return this.isEmpty;
	}
	
	public void commit() {
	}

}

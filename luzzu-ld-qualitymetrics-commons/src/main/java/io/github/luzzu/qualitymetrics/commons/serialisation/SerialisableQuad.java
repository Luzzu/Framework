package io.github.luzzu.qualitymetrics.commons.serialisation;

import java.io.Serializable;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.AnonId;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.core.Quad;

import io.github.luzzu.operations.cache.JenaCacheObject;


/**
 * @author Jeremy Debattista
 * 
 */
public class SerialisableQuad extends Quad implements Serializable, JenaCacheObject<Quad> {

	private static final long serialVersionUID = 7592196404523948594L;

	private transient Quad quad;
	
	private boolean isSubjectBlank = false;
	private String subject = "";
	
	private String predicate = "";
	
	private boolean isObjectBlank = false;
	private boolean isObjectLiteral = false;
	private String object = "";
	
	private boolean hasGraph = false;
	private String graph = "";
	
	
	public SerialisableQuad(){
		super(null, null);
	}
	
	public SerialisableQuad(Quad quad){
		super(quad.getGraph(), quad.asTriple());
		this.quad = quad;
		
		Node _sbj = this.quad.getSubject();
		if (_sbj.isBlank()) isSubjectBlank = true;
		this.subject = _sbj.toString();
		
		this.predicate = this.quad.getPredicate().toString();
		
		Node _obj = this.quad.getObject();
		if (_obj.isBlank()) isObjectBlank = true;
		if (_obj.isLiteral()) isObjectLiteral = true;
		this.object = _obj.toString();
		
		Node _graph = this.quad.getGraph();
		if (_graph != null){
			this.graph = _graph.getURI();
			this.hasGraph = true;
		}
		
	}
	
	public Triple getTriple(){
		Resource _sbj, _prd;
		RDFNode _obj;
		
		if (isSubjectBlank) _sbj = ModelFactory.createDefaultModel().createResource(new AnonId(this.subject));
		else _sbj = ModelFactory.createDefaultModel().createResource(subject);
			
		_prd = ModelFactory.createDefaultModel().createProperty(predicate);
		
		if (isObjectBlank) _obj = ModelFactory.createDefaultModel().createResource(new AnonId(this.object));
		else if (isObjectLiteral) _obj = ModelFactory.createDefaultModel().createLiteral(object);
		else _obj = ModelFactory.createDefaultModel().createResource(object);
		
		return new Triple(_sbj.asNode(), _prd.asNode(), _obj.asNode());
	}
	
	public Quad getQuad(){
		if (this.hasGraph){
			Resource _graph = ModelFactory.createDefaultModel().createResource(this.graph);
			return new Quad(_graph.asNode(), this.getTriple());
		} else 
			return new Quad(null, this.getTriple());
	}
	
	@Override
	public boolean equals(Object other){
		if (!(other instanceof SerialisableQuad)) return false;
		
		SerialisableQuad _otherSerialisableQuad = (SerialisableQuad) other;
		Quad _otherQuad = _otherSerialisableQuad.getQuad();
		
		return _otherQuad.equals(this.getQuad());
	}
	
	@Override
	public int hashCode(){
		StringBuilder sb = new StringBuilder();
		sb.append(this.subject);
		sb.append(this.predicate);
		sb.append(this.object);
		sb.append(this.graph);
		
		return sb.toString().hashCode();
	}
	
	@Override
	public String toString(){
		return "[" + this.subject + ", "+ this.predicate + ", " + this.object + "," + this.graph + "]" ;
		
	}

	@Override
	public Quad deserialise() {
		return this.getQuad();
	}
}
	
	

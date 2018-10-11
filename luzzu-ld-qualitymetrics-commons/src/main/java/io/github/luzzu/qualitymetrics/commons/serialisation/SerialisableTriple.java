package io.github.luzzu.qualitymetrics.commons.serialisation;

import java.io.Serializable;

import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.AnonId;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;

import io.github.luzzu.operations.cache.JenaCacheObject;


/**
 * @author Jeremy Debattista
 * 
 */
public class SerialisableTriple  implements Serializable, JenaCacheObject<Triple>{
	private static final long serialVersionUID = 1886611930455854430L;

	private transient Triple triple;
	
	private boolean isSubjectBlank = false;
	private String subject = "";
	
	private String predicate = "";
	
	private boolean isObjectBlank = false;
	private boolean isObjectLiteral = false;
	private String object = "";
	protected String objectDT = "";
	private transient RDFDatatype objectDataType = null;
	protected String objectLang = "";
	
	public SerialisableTriple(){}
	
	public SerialisableTriple(Triple triple){
		this.triple = triple;
		
		Node _sbj = this.triple.getSubject();
		if (_sbj.isBlank()) isSubjectBlank = true;
		this.subject = _sbj.toString();
		
		this.predicate = this.triple.getPredicate().toString();
		
		Node _obj = this.triple.getObject();
		if (_obj.isBlank()) isObjectBlank = true;
		this.object = _obj.toString();
		
		if (_obj.isLiteral()) {
			isObjectLiteral = true;
			objectDataType = _obj.getLiteralDatatype();
			objectDT = _obj.getLiteralDatatypeURI();
			objectLang = _obj.getLiteralLanguage();
			object = _obj.getLiteralLexicalForm();
		}
	}
	
	public Triple getTriple(){
		Resource _sbj, _prd;
		RDFNode _obj;
		
		if (isSubjectBlank) _sbj = ModelFactory.createDefaultModel().createResource(new AnonId(this.subject));
		else _sbj = ModelFactory.createDefaultModel().createResource(subject);
			
		_prd = ModelFactory.createDefaultModel().createProperty(predicate);
		
		if (isObjectBlank) _obj = ModelFactory.createDefaultModel().createResource(new AnonId(this.object));
		else if (isObjectLiteral){
			_obj = ModelFactory.createDefaultModel().createTypedLiteral(this.object, this.objectDataType);
			
		}
		else _obj = ModelFactory.createDefaultModel().createResource(object);
		
		return new Triple(_sbj.asNode(), _prd.asNode(), _obj.asNode());
	}
	
	@Override
	public boolean equals(Object other){
		if (!(other instanceof SerialisableTriple)) return false;
		
		SerialisableTriple _otherSerialisableTriple = (SerialisableTriple) other;
		Triple _otherTriple = _otherSerialisableTriple.getTriple();
		
		return _otherTriple.equals(this.getTriple());
	}
	
	@Override
	public int hashCode(){
		StringBuilder sb = new StringBuilder();
		sb.append(this.subject);
		sb.append(this.predicate);
		sb.append(this.object);
		
		return sb.toString().hashCode();
	}

	@Override
	public Triple deserialise() {
		return this.getTriple();
	}
}
	
	
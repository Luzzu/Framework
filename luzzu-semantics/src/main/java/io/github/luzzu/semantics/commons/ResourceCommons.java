package io.github.luzzu.semantics.commons;

import java.util.Calendar;
import java.util.Date;
import java.util.UUID;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.impl.ModelCom;
import org.apache.jena.sparql.core.Quad;

import io.github.luzzu.operations.properties.PropertyManager;


public class ResourceCommons {

	private static ModelCom mc = new ModelCom(Graph.emptyGraph);
	private static Model m = ModelFactory.createDefaultModel();
	
	private static String metadata_namespace = PropertyManager.getInstance().getProperties("luzzu.properties").getProperty("URI_NS");

	private ResourceCommons(){}
	
	public static Resource generateURI(){
		String uri = metadata_namespace;
			
		uri += UUID.randomUUID().toString();
		Resource r = m.createResource(uri);
		return r;
	}
	
	public static Resource toResource(String uri){
		Resource r = m.createResource(uri);
		return r;
	}
	
	public static Literal generateCurrentTime(){
		return m.createTypedLiteral(Calendar.getInstance());
	}
	
	public static Literal generateDoubleTypeLiteral(double d){
		return m.createTypedLiteral(d);
	}
	
	public static Literal generateBooleanTypeLiteral(boolean b){
		return m.createTypedLiteral(b);
	}
	
	public static Literal generateTypeLiteral(Object o) {
		return m.createTypedLiteral(o);
	}
	
	public static Literal generateDateLiteral(Date d) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(d);
		return m.createTypedLiteral(cal);
	}
	
	public static RDFNode generateRDFBlankNode(){
		return ModelFactory.createDefaultModel().asRDFNode(NodeFactory.createBlankNode());
	}
	
	public static RDFNode asRDFNode(Node n){
		return mc.asRDFNode(n);
	}
	
	public static Quad statementToQuad(Statement statement, Resource graph){
		return new Quad(statement.getSubject().asNode(), statement.getPredicate().asNode(), statement.getObject().asNode(), graph.asNode());
	}
}

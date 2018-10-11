package io.github.luzzu.datatypes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.sparql.core.Quad;

public class Object2Quad {

	private Quad quad;
	private Triple triple;

	final static Logger logger = LoggerFactory.getLogger(Object2Quad.class);

	public Object2Quad(Object iterator){
		if (iterator instanceof Quad){
			this.quad = (Quad) iterator;
		}
		
		if (iterator instanceof Triple){
			this.triple = (Triple) iterator;
		}
		
		if (iterator instanceof QuerySolution){
			Node s = ((QuerySolution) iterator).get("?s").asNode();
			Node p = ((QuerySolution) iterator).get("?p").asNode();
			Node o = ((QuerySolution) iterator).get("?o").asNode();

			this.triple = new Triple(s,p,o);
		}
	}
	
	public Quad getStatement(){
		if (quad == null){
			quad = new Quad(null, triple);
		}
		
		return quad;
	}
}

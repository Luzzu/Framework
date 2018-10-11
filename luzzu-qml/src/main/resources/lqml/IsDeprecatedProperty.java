package io.github.luzzu.testing.metrics;

import java.util.function.Predicate;

import org.apache.jena.graph.Node;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDF;

import io.github.luzzu.qml.datatypes.conditions.ImplementedConditions.ICustomCondition;

public class IsDeprecatedProperty implements ICustomCondition {
	/*
	 * This function expects a resource, more specifically a defined
	 * Class, to check if it is deprecated or not
	 */
	@Override
	public boolean compute(Object... args) throws IllegalArgumentException {
		if(args.length != 1) {
			throw new IllegalArgumentException("Illegal Number of Arguments, Required 1, Found "+args.length);
		}
		Node n = ((Node) args[0]);
		if (n.isURI()){
			Model m = RDFDataMgr.loadModel(n.getNameSpace());
			boolean isDeprecated = m.listObjectsOfProperty(m.createResource(n.getURI()), RDF.type).filterKeep(deprecatedFilter).hasNext();
			return isDeprecated;
		} else return false;
	}
	
	private Predicate<RDFNode> deprecatedFilter = new Predicate<RDFNode>() {
		public boolean test(RDFNode t) {
			return t.equals(OWL.DeprecatedProperty);
		}
	};

}


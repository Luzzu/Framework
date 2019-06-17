package io.github.luzzu.datatypes.r2rml;

import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.log4j.Logger;

/**
 * TriplesMap Class.
 * 
 * Based on code developed by Christophe Debruyne (https://opengogs.adaptcentre.ie/debruync/r2rml)
 * released under MIT license
 *
 */
public class GraphMap extends TermMap {

	private static Logger logger = Logger.getLogger(GraphMap.class.getName());

	public GraphMap(Resource description, String baseIRI) {
		super(description, baseIRI);
	}

	@Override
	public boolean preProcessAndValidate() {
		if (!super.preProcessAndValidate())
			return false;

		return true;
	}

	@Override
	protected RDFNode distillConstant(RDFNode node) {
		// If the constant-valued term map is a subject map, predicate map or graph map,
		// then its constant value must be an IRI.
		if (!node.isURIResource()) {
			logger.error("Constant for GraphMap must be an IRI.");
			logger.error(description);
			return null;
		}

		return node;
	}

	@Override
	protected boolean isChosenTermTypeValid() {
		if (!isTermTypeIRI()) {
			logger.error("TermType for GraphMap must be rr:IRI.");
			logger.error(description);
			return false;
		}
		return true;
	}

	@Override
	protected Resource inferTermType() {
		return R2RML.IRI;
	}

}

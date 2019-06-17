package io.github.luzzu.datatypes.r2rml;

import java.util.List;

import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.log4j.Logger;

/**
 * Join Class.
 * 
 * Based on code developed by Christophe Debruyne (https://opengogs.adaptcentre.ie/debruync/r2rml)
 * released under MIT license
 *
 */
public class Join extends R2RMLResource {

	private static Logger logger = Logger.getLogger(Join.class.getName());

	private String child;
	private String parent;

	public Join(Resource description) {
		super(description);
	}

	@Override
	public boolean preProcessAndValidate() {
		// logger.info("Processing Join " + description);

		List<Statement> list = description.listProperties(R2RML.child).toList();
		if (list.size() != 1) {
			logger.error("Join must have exactly one rr:child.");
			logger.error(description);
			return false;
		}

		RDFNode node = list.get(0).getObject();
		if (!node.isLiteral()) {
			logger.error("rr:child has to be a literal.");
			logger.error(description);
			return false;
		}

		child = node.asLiteral().toString();

		if (!R2RMLUtil.isValidColumnName(child)) {
			logger.error("rr:child is not a valid column name.");
			logger.error(description);
			return false;
		}

		list = description.listProperties(R2RML.parent).toList();
		if (list.size() != 1) {
			logger.error("Join must have exactly one rr:parent.");
			logger.error(description);
			return false;
		}

		node = list.get(0).getObject();
		if (!node.isLiteral()) {
			logger.error("rr:parent has to be a literal.");
			logger.error(description);
			return false;
		}

		parent = node.asLiteral().toString();

		if (!R2RMLUtil.isValidColumnName(parent)) {
			logger.error("rr:parent is not a valid column name.");
			logger.error(description);
			return false;
		}

		return true;
	}

	public String getChild() {
		return child;
	}

	public String getParent() {
		return parent;
	}

}

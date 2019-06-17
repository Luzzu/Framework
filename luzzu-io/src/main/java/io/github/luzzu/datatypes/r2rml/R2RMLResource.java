package io.github.luzzu.datatypes.r2rml;

import org.apache.jena.rdf.model.Resource;

/**
 * R2RMLResource Class.
 * 
 * Based on code developed by Christophe Debruyne (https://opengogs.adaptcentre.ie/debruync/r2rml)
 * released under MIT license
 *
 */
public abstract class R2RMLResource {

	abstract protected boolean preProcessAndValidate();

	protected Resource description = null;

	public R2RMLResource(Resource description) {
		this.description = description;
	}

	public Resource getDescription() {
		return description;
	}
}

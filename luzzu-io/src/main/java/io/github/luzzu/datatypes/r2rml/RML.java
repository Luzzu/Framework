package io.github.luzzu.datatypes.r2rml;

import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;

/**
 * RML Class.
 * 
 * @author Ademar Crotti Junior
 *
 */
public final class RML {

	public static final String NS = "http://semweb.mmlab.be/ns/rml#";

	// Classes
	public static final Resource LogicalSource = ResourceFactory.createResource(NS + "LogicalSource");

	// Properties
	public static final Property referenceFormulation = ResourceFactory.createProperty(NS + "referenceFormulation");
	public static final Property logicalSource = ResourceFactory.createProperty(NS + "logicalSource");
	public static final Property source = ResourceFactory.createProperty(NS + "source");
	public static final Property reference = ResourceFactory.createProperty(NS + "reference");
	public static final Property iterator = ResourceFactory.createProperty(NS + "iterator");

}

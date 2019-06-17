package io.github.luzzu.datatypes.r2rml;

import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.jena.iri.IRI;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.vocabulary.XSD;
import org.apache.log4j.Logger;

/**
 * TermMap Class.
 * 
 * Based on code developed by Christophe Debruyne (https://opengogs.adaptcentre.ie/debruync/r2rml)
 * released under MIT license
 *
 */
public abstract class TermMap extends R2RMLResource {

	private static Logger logger = Logger.getLogger(TermMap.class.getName());
	private Resource termType = null;

	/*
	 * Term generation rules for blank nodes. If the term type is rr:BlankNode: Return a blank node
	 * that is unique to the natural RDF lexical form corresponding to value. This seems to imply
	 * that there is a one-on-one mapping for each value and blank node. We will thus map the
	 * outcome of the constant, template, or column to the same blank node. In other words, if two
	 * TermMaps use "test"^^xsd:string, return same blank node. "1"^^xsd:string and "1"^^xsd:integer
	 * are different.
	 * 
	 */
	private static Map<Object, Resource> blankNodeMap = new HashMap<Object, Resource>();

	private String template;
	protected RDFNode constant;
	private String column;

	protected String language = null;
	protected Resource datatype = null;
	protected String baseIRI = null;

	public TermMap(Resource description, String baseIRI) {
		super(description);
		this.baseIRI = baseIRI;
	}

	@Override
	protected boolean preProcessAndValidate() {
		// logger.info("Processing TermMap " + description);

		List<Statement> templates = description.listProperties(R2RML.template).toList();
		List<Statement> constants = description.listProperties(R2RML.constant).toList();
		List<Statement> columns = description.listProperties(R2RML.column).toList();

		// Having exactly one of rr:constant, rr:column, rr:template
		if (templates.size() + constants.size() + columns.size() != 1) {
			logger.error("TermMap must have exactly one of rr:constant, rr:column, rr:template, and rrf:functionCall.");
			logger.error(description);
			return false;
		}

		// The value of the rr:column property must be a valid column name.
		if (columns.size() == 1) {
			column = distillColumnName(columns.get(0).getObject());
			if (column == null) {
				logger.error("The value of the rr:column property must be a valid column name.");
				logger.error(description);
				return false;
			}
		} else if (templates.size() == 1) {
			// Check whether it is a valid template
			template = distillTemplate(templates.get(0).getObject());
			if (template == null) {
				logger.error("The value of the rr:template property must be a valid string template.");
				logger.error(description);
				return false;
			}

			// Check whether the referenced column names are valid
			for (String columnName : getReferencedColumns()) {
				if (!R2RMLUtil.isValidColumnName(columnName)) {
					logger.error("Invalid column name in rr:template: " + columnName);
					logger.error(description);
					return false;
				}
			}
		} else if (constants.size() == 1) {
			// the check for ConstantValuedTermMaps are local (different rules
			// for different TermMaps.
			constant = distillConstant(constants.get(0).getObject());
			if (constant == null)
				return false;
		}

		// Validity of the termType is also local.
		// At most one and compute default one if absent.
		List<Statement> list = description.listProperties(R2RML.termType).toList();
		if (list.size() > 1) {
			logger.error("TermMap can have at most one rr:termType.");
			logger.error(description);
			return false;
		} else if (list.size() == 0) {
			termType = inferTermType();
		} else {
			// We have exactly one value. Check validity.
			// Is it a valid IRI?
			if (!list.get(0).getObject().isURIResource()) {
				logger.error("TermMap's rr:termType must be a valid IRI.");
				logger.error(description);
				return false;
			}

			termType = list.get(0).getObject().asResource();
			// Is it a valid option?
			if (!isChosenTermTypeValid())
				return false;
		}

		return true;
	}

	/**
	 * Infer "default" termtype.
	 * 
	 * @return
	 */
	protected abstract Resource inferTermType();

	/**
	 * True if chosen TermType is valid for subclasses.
	 * 
	 * @return
	 */
	protected abstract boolean isChosenTermTypeValid();

	/**
	 * Returns RDF term if the conditions for constant values for one of the TermMap's subclasses
	 * are met. Only to be called if a TermMap has a constant.
	 * 
	 * @return
	 */
	protected abstract RDFNode distillConstant(RDFNode node);

	private String distillTemplate(RDFNode node) {
		if (!node.isLiteral())
			return null;
		if (!node.asLiteral().getDatatype().getURI().equals(XSD.xstring.getURI()))
			return null;
		// TODO: check the actual value of the template
		return node.asLiteral().toString();
	}

	private Set<String> getReferencedColumns() {
		Set<String> set = new HashSet<String>();
		if (isColumnValuedTermMap()) {
			// Singleton
			set.add(column);
		} else if (isTemplateValuedTermMap()) {
			// Little hack to "ignore" escaped curly braces.
			String temp = template.replace("\\{", "--").replace("\\}", "--");
			Matcher m = Pattern.compile("\\{([^}]+)\\}").matcher(temp);
			while (m.find()) {
				set.add(template.substring(m.start(1), m.end(1)));
			}
		} // else constant and thus empty set.
		return set;
	}

	private String distillColumnName(RDFNode node) {
		if (!node.isLiteral())
			return null;
		if (!node.asLiteral().getDatatype().getURI().equals(XSD.xstring.getURI()))
			return null;
		String s = node.asLiteral().toString();
		if (!R2RMLUtil.isValidColumnName(s))
			return null;
		return s;
	}

	public boolean isTemplateValuedTermMap() {
		return template != null;
	}

	public boolean isColumnValuedTermMap() {
		return column != null;
	}

	public boolean isConstantValuedTermMap() {
		return constant != null;
	}

	public Resource getTermType() {
		return termType;
	}

	public boolean isTermTypeBlankNode() {
		return getTermType().getURI().equals(R2RML.BLANKNODE.getURI());
	}

	public boolean isTermTypeIRI() {
		return getTermType().getURI().equals(R2RML.IRI.getURI());
	}

	public boolean isTermTypeLiteral() {
		return getTermType().getURI().equals(R2RML.LITERAL.getURI());
	}

	/**
	 * Private method to create safe IRIs. It does percent encoding. According to the R2RML
	 * Standard, however, some characters (like kanji) should not be percent encoded. This is what
	 * jena does. We need to find a library that better complies with the standard.
	 * 
	 * 42 -> 42 OK Hello World! -> Hello%20World%21 OK 2011-08-23T22:17:00Z ->
	 * 2011-08-23T22%3A17%3A00Z OK ~A_17.1-2 -> ~A_17.1-2 OK 葉篤正 -> 葉篤正 NOK!
	 * 
	 * TODO: Better compliant safe IRI conversion.
	 * 
	 * @param iri
	 * @return
	 * @throws R2RMLException
	 */
	private String convertToIRISafeVersion(IRI iri) throws R2RMLException {
		try {
			return iri.toASCIIString();
		} catch (MalformedURLException e) {
			throw new R2RMLException("Problem generating safe IRI " + iri, e);
		}
	}

	public String getColumn() {
		return column;
	}

	public String getTemplate() {
		return template;
	}

	public RDFNode getConstant() {
		return constant;
	}
}

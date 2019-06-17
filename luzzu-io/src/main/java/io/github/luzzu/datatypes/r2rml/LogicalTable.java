package io.github.luzzu.datatypes.r2rml;

import java.util.List;

import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.XSD;
import org.apache.log4j.Logger;

/**
 * LogicalTable Class.
 * 
 * Based on code developed by Christophe Debruyne (https://opengogs.adaptcentre.ie/debruync/r2rml)
 * released under MIT license
 *
 */
public class LogicalTable extends R2RMLResource {

	private static Logger logger = Logger.getLogger(LogicalTable.class.getName());

	private String tableName = null;
	private String sqlQuery = null;

	public LogicalTable(Resource description) {
		super(description);
	}

	@Override
	public boolean preProcessAndValidate() {
		// logger.info("Processing LogicalTable " + description);

		// Minimum conditions
		// LogicalTable: Being one of its subclasses, rr:BaseTableOrView or rr:R2RMLView
		// BaseTableOrView: Having an rr:tableName property
		// R2RMLView: Having an rr:sqlQuery property

		boolean isBaseTableOrView = false;
		boolean isR2RMLView = false;
		List<Statement> list = description.listProperties(RDF.type).toList();
		for (Statement s : list) {
			if (s.getObject().equals(R2RML.BaseTableOrView))
				isBaseTableOrView = true;
			if (s.getObject().equals(R2RML.R2RMLView))
				isR2RMLView = true;
		}

		// We will not explicitly check whether it is a LogicalTable as it has
		// to be one of its subclasses and -- via inference -- a LogicalTable.
		// Perform XOR to see if it is exactly one of its subclasses.
		if (!(isBaseTableOrView ^ isR2RMLView)) {
			logger.error("LogicalTable must be exactly one of its subclasses.");
			logger.error(description);
			return false;
		}

		if (isBaseTableOrView) {
			// Check cardinality
			List<Statement> l = description.listProperties(R2RML.tableName).toList();
			if (l.size() != 1) {
				logger.error("BaseTableOrView must have exactly one rr:tableName.");
				logger.error(description);
				return false;
			}

			// Check value
			RDFNode n = l.get(0).getObject();

			if (!n.isLiteral() || !n.asLiteral().getDatatype().getURI().equals(XSD.xstring.getURI())) {
				logger.error("TableName is not a valid xsd:string.");
				logger.error(description);
				return false;
			}

			// All clear
			tableName = n.toString();

		} else {
			// Check cardinality
			List<Statement> l = description.listProperties(R2RML.sqlQuery).toList();
			if (l.size() != 1) {
				logger.error("R2RMLView must have exactly one rr:sqlQuery.");
				logger.error(description);
				return false;
			}

			// Check value
			RDFNode n = l.get(0).getObject();
			if (!n.isLiteral() || !n.asLiteral().getDatatype().getURI().equals(XSD.xstring.getURI())) {
				logger.error("SqlQuery is not a valid xsd:string.");
				logger.error(description);
				return false;
			}

			// All clear
			sqlQuery = n.toString().trim();

			// Some relational databases do not support semicolons at the end
			// of a subquery, so remove one when it appears. Technically, one
			// can have many such semicolons (with white spaces), but we start
			// by removing one.
			if (sqlQuery.endsWith(";"))
				sqlQuery = sqlQuery.substring(0, sqlQuery.length() - 1);
		}

		return true;
	}

	public String generateQuery() {
		if (sqlQuery != null)
			return sqlQuery;
		return "SELECT * FROM " + tableName;
	}

	public String getTableName() {
		return tableName;
	}

	public String getSqlQuery() {
		return sqlQuery;
	}

}

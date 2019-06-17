package io.github.luzzu.datatypes.r2rml;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.jena.rdf.model.RDFNode;
import org.apache.log4j.Logger;

/**
 * R2RMLUtil Class.
 * 
 * Based on code developed by Christophe Debruyne (https://opengogs.adaptcentre.ie/debruync/r2rml)
 * released under MIT license
 *
 */

public class R2RMLUtil {

	private static Logger logger = Logger.getLogger(R2RMLUtil.class.getName());

	public static boolean isValidLanguageTag(RDFNode node) {
		if (!node.isLiteral())
			return false;
		// TODO: Implement language tag check
		return true;
	}

	/**
	 * Utility function to check whether the string denoting a column name is valid
	 * 
	 * @param columnName
	 * @return True if a valid column name
	 */
	public static boolean isValidColumnName(String columnName) {
		// TODO: check the actual value of the column is valid
		return true;
	}

	public static String createJointQuery(TriplesMap child, TriplesMap parent, List<Join> joins) {
		// If the child query and parent query of a referencing object
		// map are not identical, then the referencing object map must
		// have at least one join condition.

		String cquery = child.getLogicalTable().generateQuery();
		String pquery = parent.getLogicalTable().generateQuery();

		if (!cquery.equals(pquery) && joins.isEmpty()) {
			logger.error("If the child query and parent query of a referencing object map are not identical, then the referencing object map must have at least one join condition.");
			return null;
		}

		// If the referencing object map has no join condition
		if (joins.isEmpty())
			return "SELECT * FROM (" + cquery + ") AS tmp";

		String query = "SELECT * FROM (" + cquery + ") AS child, ";
		query += "(" + pquery + ") AS parent WHERE ";

		for (Join join : joins) {
			query += "child." + join.getChild() + "=";
			query += "parent." + join.getParent() + " AND ";
		}

		query += "TRUE";

		return query;
	}

	public static String createJointQueryPreview(TriplesMap triplesMap, TriplesMap ptm, List<Join> joins, Map<String, Object> data) {

		String query = createJointQuery(triplesMap, ptm, joins);

		for (Join join : joins) {
			query += " AND child." + join.getChild() + "=" + data.get(join.getChild());
		}

		return query;
	}

	public static Map<String, Object> getAttributeValueMap(ResultSet resultset) {
		Map<String, Object> attributeValueMap = new HashMap<String, Object>();
		try {
			ResultSetMetaData rsmd = resultset.getMetaData();
			int columnCount = rsmd.getColumnCount();
			for (int i = 1; i <= columnCount; i++) {
				String name = rsmd.getColumnName(i);
				attributeValueMap.put(name, resultset.getObject(i));
			}
		} catch (SQLException e) {
			logger.error("Error processing resultset metadata: " + e.getMessage());
		}

		return attributeValueMap;
	}
}

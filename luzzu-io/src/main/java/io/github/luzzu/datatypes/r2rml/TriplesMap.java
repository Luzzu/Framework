package io.github.luzzu.datatypes.r2rml;

import java.util.ArrayList;
import java.util.List;

import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.log4j.Logger;

/**
 * TriplesMap Class.
 * 
 * Based on code developed by Christophe Debruyne (https://opengogs.adaptcentre.ie/debruync/r2rml)
 * released under MIT license
 *
 */
public class TriplesMap extends R2RMLResource {

	private static Logger logger = Logger.getLogger(TriplesMap.class.getName());

	private LogicalTable logicalTable = null;
	private SubjectMap subjectMap = null;
	private List<PredicateObjectMap> predicateObjectMaps = new ArrayList<PredicateObjectMap>();
	private String baseIRI = null;

	private int count = 0;

	public TriplesMap(Resource description, String baseIRI) {
		super(description);
		this.setBaseIRI(baseIRI);
	}

	public LogicalTable getLogicalTable() {
		return logicalTable;
	}

	public void setLogicalTable(LogicalTable logicalTable) {
		this.logicalTable = logicalTable;
	}

	public SubjectMap getSubjectMap() {
		return subjectMap;
	}

	public void setSubjectMap(SubjectMap subjectMap) {
		this.subjectMap = subjectMap;
	}

	public List<PredicateObjectMap> getPredicateObjectMaps() {
		return predicateObjectMaps;
	}

	public void setPredicateObjectMaps(List<PredicateObjectMap> predicateObjectMaps) {
		this.predicateObjectMaps = predicateObjectMaps;
	}

	@Override
	public boolean preProcessAndValidate() {
		// logger.info("Processing TriplesMap " + description);

		// TermMap must have an rr:logicalTable property (exactly one?)
		List<Statement> list = description.listProperties(R2RML.logicalTable).toList();

		if (list.isEmpty()) {
			list = description.listProperties(RML.logicalSource).toList();
		}

		if (list.size() != 1) {
			logger.error("TriplesMap must have exactly one rr:logicalTable or one rml:logicalSource property.");
			logger.error(description);
			return false;
		}

		RDFNode node = list.get(0).getObject();
		if (!node.isResource()) {
			logger.error("LogicalTable of TriplesMap is not a resource.");
			logger.error(description);
			return false;
		}

		// Pre-process and validate LogicalTable
		logicalTable = new LogicalTable(node.asResource());
		if (!logicalTable.preProcessAndValidate())
			return false;

		// TermMap must have exactly one of rr:subject and rr:subjectMap
		// But we constructed rr:subjectMap from rr:subject, thus only check
		// one!
		list = description.listProperties(R2RML.subjectMap).toList();
		if (list.size() != 1) {
			logger.error("TriplesMap must have exactly one one of rr:subject and rr:subjectMap.");
			logger.error(description);
			return false;
		}

		node = list.get(0).getObject();
		if (!node.isResource()) {
			logger.error("SubjectMap of TriplesMap is not a resource.");
			logger.error(description);
			return false;
		}

		// Pre-process and validate SubjectMap
		subjectMap = new SubjectMap(node.asResource(), baseIRI);
		if (!subjectMap.preProcessAndValidate())
			return false;

		// Pre-process and validate PredicateObjectMaps
		// TriplesMaps may have zero or more PredicateObjectMaps
		// Just iterate over them.
		list = description.listProperties(R2RML.predicateObjectMap).toList();
		for (Statement s : list) {
			node = s.getObject();
			if (!node.isResource()) {
				logger.error("PredicateObjectMap is not a resource.");
				logger.error(description);
				return false;
			}

			PredicateObjectMap opm = new PredicateObjectMap(s.getObject().asResource(), baseIRI);
			if (!opm.preProcessAndValidate())
				return false;
			predicateObjectMaps.add(opm);
		}

		return true;
	}

	public String getBaseIRI() {
		return baseIRI;
	}

	public void setBaseIRI(String baseIRI) {
		this.baseIRI = baseIRI;
	}

}

package io.github.luzzu.datatypes.r2rml;

import java.util.HashMap;
import java.util.Map;

import org.apache.jena.rdf.model.Resource;

/**
 * R2RMLMapping Class.
 * 
 * Based on code developed by Christophe Debruyne (https://opengogs.adaptcentre.ie/debruync/r2rml)
 * released under MIT license
 *
 */
public class R2RMLMapping {

	private Map<Resource, TriplesMap> triplesMaps = new HashMap<Resource, TriplesMap>();

	public void addTriplesMap(Resource triplesMapResource, TriplesMap triplesMap) {
		triplesMaps.put(triplesMapResource, triplesMap);
	}

	public Map<Resource, TriplesMap> getTriplesMaps() {
		return triplesMaps;
	}

	public void setTriplesMaps(Map<Resource, TriplesMap> triplesMaps) {
		this.triplesMaps = triplesMaps;
	}

}

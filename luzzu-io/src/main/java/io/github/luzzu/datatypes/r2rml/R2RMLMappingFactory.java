package io.github.luzzu.datatypes.r2rml;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;

import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.rdf.model.InfModel;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.util.FileManager;
import org.apache.jena.vocabulary.RDF;
import org.apache.log4j.Logger;

/**
 * R2RMLMappingFactory Class.
 * 
 * Based on code developed by Christophe Debruyne (https://opengogs.adaptcentre.ie/debruync/r2rml)
 * released under MIT license
 *
 */
public class R2RMLMappingFactory {

	private static Logger logger = Logger.getLogger(R2RMLMappingFactory.class.getName());

	private static String CONSTRUCTSMAPS = "PREFIX rr: <http://www.w3.org/ns/r2rml#> CONSTRUCT { ?x rr:subjectMap [ rr:constant ?y ]. } WHERE { ?x rr:subject ?y. }";
	private static String CONSTRUCTOMAPS = "PREFIX rr: <http://www.w3.org/ns/r2rml#> CONSTRUCT { ?x rr:objectMap [ rr:constant ?y ]. } WHERE { ?x rr:object ?y. }";
	private static String CONSTRUCTPMAPS = "PREFIX rr: <http://www.w3.org/ns/r2rml#> CONSTRUCT { ?x rr:predicateMap [ rr:constant ?y ]. } WHERE { ?x rr:predicate ?y. }";
	private static String CONSTRUCTGMAPS = "PREFIX rr: <http://www.w3.org/ns/r2rml#> CONSTRUCT { ?x rr:graphMap [ rr:constant ?y ]. } WHERE { ?x rr:graph ?y. }";

	// MAKE CONSTANTS EXPLICITELY LITERALS TO DEAL WITH TYPED CONSTANTS
	// EVEN THOUGH IT IS OUT OF THE SPEC
	private static String CONSTRUCTLITERAL = "PREFIX rr: <http://www.w3.org/ns/r2rml#> CONSTRUCT { ?x rr:termType rr:Literal . } WHERE { ?x rr:constant ?y . FILTER (isLiteral(?y)) }";

	// If mapping is contained in a String (in TURTLE)
	public static R2RMLMapping createR2RMLMappingFromString(String mapping, String baseIRI) {
		Model data = ModelFactory.createDefaultModel();
		data.read(new ByteArrayInputStream(mapping.getBytes()), null, "TURTLE");
		return createR2RMLMapping(data, baseIRI);
	}

	// If mapping is contained in a Input Stream (in TURTLE)
	public static R2RMLMapping createR2RMLMappingFromInputStream(InputStream mapping, String baseIRI) {
		Model data = ModelFactory.createDefaultModel();
		data.read(mapping, null, "TURTLE");
		return createR2RMLMapping(data, baseIRI);
	}

	// If mapping is contained in file
	public static R2RMLMapping createR2RMLMappingFromFile(String mappingFile, String baseIRI) {
		Model data = FileManager.get().loadModel(mappingFile);
		return createR2RMLMapping(data, baseIRI);
	}

	private static R2RMLMapping createR2RMLMapping(Model data, String baseIRI) {
		R2RMLMapping mapping = new R2RMLMapping();

		// We reason over the mapping to facilitate retrieval of the mappings
		// We construct triples to replace the shortcuts.
		data.add(QueryExecutionFactory.create(CONSTRUCTSMAPS, data).execConstruct());
		data.add(QueryExecutionFactory.create(CONSTRUCTOMAPS, data).execConstruct());
		data.add(QueryExecutionFactory.create(CONSTRUCTPMAPS, data).execConstruct());
		data.add(QueryExecutionFactory.create(CONSTRUCTGMAPS, data).execConstruct());

		data.add(QueryExecutionFactory.create(CONSTRUCTLITERAL, data).execConstruct());

		Model schema = ModelFactory.createDefaultModel();
		schema.read(R2RMLMappingFactory.class.getResourceAsStream("/r2rml.rdf"), null);

		// Model schema = FileManager.get().loadModel("./resources/r2rml.rdf");
		InfModel mappingmodel = ModelFactory.createRDFSModel(schema, data);

		// Look for the TriplesMaps
		List<Resource> list = mappingmodel.listSubjectsWithProperty(RDF.type, R2RML.TriplesMap).toList();

		if (list.isEmpty()) {
			logger.error("R2RML Mapping File has no TriplesMaps.");
			return null;
		}

		for (Resource tm : list) {
			TriplesMap triplesMap = new TriplesMap(tm, baseIRI);
			if (!triplesMap.preProcessAndValidate()) {
				// Something went wrong, abort.
				return null;
			}
			mapping.addTriplesMap(tm, triplesMap);
		}

		return mapping;
	}

	/**
	 * Small utility function to test URL based on
	 * http://stackoverflow.com/questions/1600291/validating-url-in-java
	 * 
	 * @param uri
	 * @return
	 */
	private static boolean isValidURL(String uri) {
		try {
			URL u = new URL(uri);
			u.toURI();
		} catch (MalformedURLException e) {
			return false;
		} catch (URISyntaxException e) {
			return false;
		}
		return true;
	}

}

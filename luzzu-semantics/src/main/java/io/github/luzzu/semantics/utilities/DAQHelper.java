package io.github.luzzu.semantics.utilities;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.jena.riot.RDFDataMgr;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.ResIterator;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.vocabulary.DC;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.luzzu.operations.lowlevel.ExceptionOutput;
import io.github.luzzu.semantics.configuration.InternalModelConf;
import io.github.luzzu.semantics.datatypes.Observation;
import io.github.luzzu.semantics.vocabularies.CUBE;
import io.github.luzzu.semantics.vocabularies.DAQ;

public class DAQHelper {
	
	protected final static Logger logger = LoggerFactory.getLogger(DAQHelper.class);

	private DAQHelper(){}
	
	public static String getClassLabel(Resource uri){
		StmtIterator iter = InternalModelConf.getDAQModel().listStatements(uri, RDFS.label, (RDFNode) null);
		String label = "";
		while (iter.hasNext()){
			label = iter.nextStatement().getObject().toString();
		}
		return label;
	}
	
	public static String getDimensionLabel(Resource metricURI){
		return getClassLabel(getDomainResource(metricURI));
	}
	
	public static String getCategoryLabel(Resource metricURI){
		Resource dim = getDomainResource(metricURI);
		Resource cat = getDomainResource(dim);
		
		return getClassLabel(cat);
	}
	
	public static Resource getDimensionResource(Resource metricURI){
		return getDomainResource(metricURI);
	}
		
	public static Resource getCategoryResource(Resource metricURI){
		Resource intermediate = getDomainResource(metricURI);
		return getDomainResource(intermediate);
	}
	
	private static Resource getDomainResource(Resource uri){
		String whereClause = "?prop " + " " + SPARQLHelper.toSPARQL(RDFS.range) + SPARQLHelper.toSPARQL(uri) + " . ";
		whereClause = whereClause + " ?prop " + SPARQLHelper.toSPARQL(RDFS.domain) + " ?domain .";
		
		Model m = InternalModelConf.getFlatModel();
		String query = SPARQLHelper.SELECT_STATEMENT.replace("[variables]", "?domain").replace("[whereClauses]", whereClause);
		Resource r = null;
		Query qry = QueryFactory.create(query);
	    QueryExecution qe = QueryExecutionFactory.create(qry, m);
	    ResultSet rs = qe.execSelect();

	    while (rs.hasNext()){
	    	r = rs.next().get("domain").asResource();
	    }
	    
	    return r;
	}
	
	public static Resource getPropertyResource(Resource uri){
		String whereClause = "?prop " + " " + SPARQLHelper.toSPARQL(RDFS.range) + SPARQLHelper.toSPARQL(uri) + " . ";
		
		Model m = InternalModelConf.getFlatModel();
		String query = SPARQLHelper.SELECT_STATEMENT.replace("[variables]", "?prop").replace("[whereClauses]", whereClause);
		Resource r = null;
		Query qry = QueryFactory.create(query);
	    QueryExecution qe = QueryExecutionFactory.create(qry, m);
	    ResultSet rs = qe.execSelect();
	    
	    while (rs.hasNext()){
	    	r = rs.next().get("prop").asResource();
	    }
	    
	    return r;
	}
	
	public static String getClassDescription(Resource uri){
		StmtIterator iter = InternalModelConf.getDAQModel().listStatements(uri, RDFS.comment, (RDFNode) null);
		String label = "";
		while (iter.hasNext()){
			label = iter.nextStatement().getObject().toString();
		}
		return label;
	}


	/**
	 * Extract all observations from a dataset
	 * @param dataset URI
	 * @return a HashMap with the metric type being the key and a list of observations
	 */
	public static Map<String,List<Observation>> getQualityMetadataObservations(String datasetMetadataUri){
		Dataset d = RDFDataMgr.loadDataset(datasetMetadataUri);
		Resource graph = d.getDefaultModel().listSubjectsWithProperty(RDF.type, DAQ.QualityGraph).next();
		Model qualityMD = d.getNamedModel(graph.getURI());
		
		Map<String,List<Observation>> map = new HashMap<String,List<Observation>>(); // metric resource, list<observations>
		
		ResIterator iter = qualityMD.listResourcesWithProperty(RDF.type, CUBE.Observation);
		while(iter.hasNext()){
			Resource res = iter.next();
			
			//get metric uri
			Resource metricURI = qualityMD.listObjectsOfProperty(res, DAQ.metric).next().asResource();
			//get metric type
			Resource metricType = qualityMD.listObjectsOfProperty(metricURI, RDF.type).next().asResource();
			
			//get datetime
			Date date = null;
			try {
				date = toDateFormat(qualityMD.listObjectsOfProperty(res, DC.date).next().asLiteral().getValue().toString());
			} catch (ParseException e) {
				ExceptionOutput.output(e, "[DAQ Helper] - Error in generating date in correct format for quality metadata", logger);
			}
			
			//get value
			Double value = qualityMD.listObjectsOfProperty(res, DAQ.value).next().asLiteral().getDouble();
			
			
			//get computedOn
			Resource computedOn = qualityMD.listObjectsOfProperty(res,DAQ.computedOn).next().asResource();
			
			//data cube
			Resource cubeDS = qualityMD.listObjectsOfProperty(res, CUBE.dataSet).next().asResource();
			
			//is estimate
			Boolean isEstimate = qualityMD.listObjectsOfProperty(res, DAQ.isEstimate).next().asLiteral().getBoolean();
			
			Observation obs = new Observation(res, date, value, value.getClass().getSimpleName(), computedOn, cubeDS, isEstimate);
			
			if (!(map.containsKey(metricType.toString()))){
				map.put(metricType.toString(), new ArrayList<Observation>());
			}
			map.get(metricType.toString()).add(obs);
		}
		
		return map;
	}
	
	/**
	 * Queries the internal model for the total number of metrics in each dimension.
	 * 
	 * @return a Map of dimensions and the count of metrics.
	 */
	public static Map<String, Integer> getNumberOfMetricsInDimension(){
		Map<String, Integer> metricsPerDimension = new HashMap<String, Integer>();
		Model m = InternalModelConf.getFlatModel();
		
		String whereClause = "?dimensionURI a " + SPARQLHelper.toSPARQL(DAQ.Dimension) + 
				" . ?dimensionURI ?hasMetricProperty ?metricURI . " +
				"?hasMetricProperty " + SPARQLHelper.toSPARQL(RDFS.subPropertyOf) + SPARQLHelper.toSPARQL(DAQ.hasMetric) + " .";
		
		String variables = "?dimensionURI COUNT(?metricURI) as ?count";
		
		String query = SPARQLHelper.SELECT_STATEMENT.replace("[variables]", variables).replace("[whereClauses]", whereClause);
		Query qry = QueryFactory.create(query);
	    QueryExecution qe = QueryExecutionFactory.create(qry, m);
	    ResultSet rs = qe.execSelect();
	    
	    while (rs.hasNext()){
	    	QuerySolution qs = rs.next();
	    	String dim = qs.get("dimensionURI").asResource().getURI();
	    	Integer count = qs.get("count").asLiteral().getInt();
	    	metricsPerDimension.put(dim, count);
	    }
		
	    return metricsPerDimension;
	}
	
	public static String getDimensionForMetric(Resource metricURI){
		Model m = InternalModelConf.getFlatModel();
		
		String whereClause = "?dimension ?prop " + SPARQLHelper.toSPARQL(metricURI) +
				"?prop " + SPARQLHelper.toSPARQL(RDFS.subPropertyOf) + SPARQLHelper.toSPARQL(DAQ.hasMetric) + " .";
				
		String query = SPARQLHelper.SELECT_STATEMENT.replace("[variables]", "?dimension").replace("[whereClauses]", whereClause);
		Query qry = QueryFactory.create(query);
	    QueryExecution qe = QueryExecutionFactory.create(qry, m);
	    ResultSet rs = qe.execSelect();
	    
	    String dim = "";
	    while (rs.hasNext()){
	    	QuerySolution qs = rs.next();
	    	dim = qs.get("dimension").asResource().getURI();
	    }
		
	    return dim;
	}
	
	public static Map<String, Integer> getNumberOfDimensionsInCategory(){
		Map<String, Integer> dimensionPerCategory = new HashMap<String, Integer>();
		Model m = InternalModelConf.getFlatModel();
		
		String whereClause = "?categoryURI a " + SPARQLHelper.toSPARQL(DAQ.Category) + 
				" . ?categoryURI ?hasDimensionProperty ?dimensionURI . " +
				"?hasMetricProperty " + SPARQLHelper.toSPARQL(RDFS.subPropertyOf) + SPARQLHelper.toSPARQL(DAQ.hasDimension) + " .";
		
		String variables = "?categoryURI COUNT(?dimensionURI) as ?count";
		
		String query = SPARQLHelper.SELECT_STATEMENT.replace("[variables]", variables).replace("[whereClauses]", whereClause);
		Query qry = QueryFactory.create(query);
	    QueryExecution qe = QueryExecutionFactory.create(qry, m);
	    ResultSet rs = qe.execSelect();
	    
	    while (rs.hasNext()){
	    	QuerySolution qs = rs.next();
	    	String cat = qs.get("categoryURI").asResource().getURI();
	    	Integer count = qs.get("count").asLiteral().getInt();
	    	dimensionPerCategory.put(cat, count);
	    }
		
	    return dimensionPerCategory;
	}
	
	public static String getCategoryForDimension(Resource dimensionURI){
		Model m = InternalModelConf.getFlatModel();
		
		String whereClause = "?category ?prop " + SPARQLHelper.toSPARQL(dimensionURI) +
				"?prop " + SPARQLHelper.toSPARQL(RDFS.subPropertyOf) + SPARQLHelper.toSPARQL(DAQ.hasDimension) + " .";
				
		String query = SPARQLHelper.SELECT_STATEMENT.replace("[variables]", "?category").replace("[whereClauses]", whereClause);
		Query qry = QueryFactory.create(query);
	    QueryExecution qe = QueryExecutionFactory.create(qry, m);
	    ResultSet rs = qe.execSelect();
	    
	    String cat = "";
	    while (rs.hasNext()){
	    	QuerySolution qs = rs.next();
	    	cat = qs.get("category").asResource().getURI();
	    }
		
	    return cat;
	}
	
	public static List<String> getDimensionsInCategory(Resource categoryURI){
		Model m = InternalModelConf.getFlatModel();
		
		String whereClause = SPARQLHelper.toSPARQL(categoryURI) + " ?prop ?dimensionURI" +
				"?prop " + SPARQLHelper.toSPARQL(RDFS.subPropertyOf) + SPARQLHelper.toSPARQL(DAQ.hasDimension) + " .";
				
		String query = SPARQLHelper.SELECT_STATEMENT.replace("[variables]", "?dimensionURI").replace("[whereClauses]", whereClause);
		Query qry = QueryFactory.create(query);
	    QueryExecution qe = QueryExecutionFactory.create(qry, m);
	    ResultSet rs = qe.execSelect();
	    
	    List<String> dimensions = new ArrayList<String>();
	    while (rs.hasNext()){
	    	QuerySolution qs = rs.next();
	    	dimensions.add(qs.get("dimensionURI").asResource().getURI());
	    }
		
	    return dimensions;
	}
	
	/**
	 * Formats an xsd:dateTime to a JAVA object data
	 * @param date - A string extracted from the triple's object
	 * @return JAVA object data
	 * 
	 * @throws ParseException
	 */
	private static Date toDateFormat(String date) throws ParseException{
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSS'Z'");
		return sdf.parse(date);
	}
}